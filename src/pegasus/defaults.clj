(ns pegasus.defaults
  "Contains default components"
  (:require [bigml.sketchy.bloom :as bloom]
            [clj-http.client :as client]
            [clj-robots.core :as robots]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [me.raynes.fs :as fs]
            [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.cache :as cache]
            [pegasus.queue :as queue]
            [pegasus.state]
            [schema.core :as s])
  (:import [clojure.lang PersistentQueue]
           [java.io StringReader]))

(defn get-request
  [url user-agent]
  (println :getting url)
  (client/get url {:socket-timeout 1000
                   :conn-timeout 1000
                   :headers {"User-Agent" user-agent}}))

(defn default-frontier-fn
  "The default frontier issues a GET request
  to the URL"
  [url]
  {:url  url
   :body (-> url
             (get-request (:user-agent pegasus.state/config))
             :body)
   :time (-> (t/now)
             c/to-long)})

(defn default-extractor-fn
  "Default extractor extracts URLs from anchor tags in
  a page"
  [obj]
  (println :extracting (:url obj))
  (let [anchor-tags (-> obj
                        :body
                        (StringReader.)
                        html/html-resource
                        (html/select [:a]))
        
        url         (:url obj)

        can-follow  (filter
                     #(-> %
                          :attrs
                          :rel
                          (not= "nofollow"))
                     anchor-tags)

        uris        (map
                     #(->> %
                           :attrs
                           :href)
                     can-follow)

        clean-uris  (filter identity uris)
        
        extracted   {:extracted (filter
                                 (fn [x]
                                   (= (uri/host x) "blog.shriphani.com"))
                                 (map #(uri/resolve-uri url %)
                                      clean-uris))}]
    (merge obj extracted)))

(defn default-writer-fn
  "The default writer pretty prints the input object
  to a corpus file."
  [obj]
  (locking (:state pegasus.state/config)
   (let [wrtr (:writer @(:state pegasus.state/config))]
    
     ;; open the writer if not already opened.
     (when (nil? wrtr)
       (swap! (:state pegasus.state/config)
              (fn [x]
                (merge-with + x {:writer (io/writer
                                          (io/file (:corpus-dir pegasus.state/config)
                                                   "corpus.clj"))}))))))
    
    ;; now try again :) - ugly code I know :)

  (let [wrtr (:writer @(:state pegasus.state/config))]
    
    (.write wrtr (str (clojure.pprint/write obj :stream nil)
                      "\n"))
    (.flush wrtr))
  obj)

(defn default-visited-check
  [obj queue visited]
  (not
   (or (some #{(:url obj)}
             visited)
       (contains? queue
                  (:url obj)))))

(defn default-bloom-update-fn
  [bloom-filter url]
  (bloom/insert bloom-filter url))

(def default-location-config
  {:job-dir nil
   :corpus-dir "corpus"
   :struct-dir "data-structures"
   :logs-dir "logs"})

(defn mkdir-if-not-exists
  [path]
  (let [a-file (io/file path)]
   (when-not (.exists a-file)
     (fs/mkdir (.getPath a-file)))))

(defn add-location-config
  "Given a user-config, returns a new config
  with location info added to it"
  [user-config]
  (let [specified-job-dir (io/file
                           (or (:job-dir user-config)
                               "/tmp"))

        corpus-dir (.getPath
                    (fs/file specified-job-dir
                             (:corpus-dir default-location-config)))
        struct-dir (.getPath
                    (fs/file specified-job-dir
                             (:struct-dir default-location-config)))
        logs-dir (.getPath
                  (fs/file specified-job-dir
                           (:logs-dir default-location-config)))]
    (mkdir-if-not-exists specified-job-dir)
    (mkdir-if-not-exists corpus-dir)
    (mkdir-if-not-exists struct-dir)
    (mkdir-if-not-exists logs-dir)
    
    (merge user-config
           {:job-dir specified-job-dir
            :corpus-dir corpus-dir
            :struct-dir struct-dir
            :logs-dir logs-dir})))

(defn add-structs-config
  [user-config]
  (let [cache-config (cache/initialize-caches user-config)]
    (merge user-config cache-config)))

(defn default-update-state
  "Update caches, insert new URLs"
  [obj]
  (let [src-url (:url obj)

        to-visit-cache (:to-visit-cache pegasus.state/config)
        visited-cache (:visited-cache pegasus.state/config)
        hosts-visited-cache (:hosts-visited-cache pegasus.state/config)

        extracted-uris (:extracted obj)]

    ;; cache updates
    (cache/remove-from-cache src-url to-visit-cache)
    (cache/add-to-cache src-url visited-cache)
    (cache/add-to-cache (uri/host src-url) hosts-visited-cache)

    ;; :state updates
    (swap! (:state pegasus.state/config)
           (fn [x]
             (merge-with + x {:num-visited 1})))

    obj))

(defn default-stop-check
  "Stops at 100 documents. This
  function is part of the crawl
  pipeline but doesn't use any of the objects being
  passed through it; it only looks at
  the crawl-state."
  [& _]
  (let [crawl-state (:state pegasus.state/config)
        num-visited (:num-visited @crawl-state)]
    (println :num-visited num-visited)
    (when (<= 100 num-visited)
      (let [init-chan (:init-chan pegasus.state/config)
            stop-sequence (:stop-sequence pegasus.state/config)]
        
        ;; do not accept any more URIs
        (async/close! init-chan)

        (println :stopping-crawl!)

        ;; any destructors needed are placed
        ;; here during the crawl phase
        (when stop-sequence
         (doseq [stop-fn (stop-sequence)]
           (stop-fn)))))))

(defn robots-filter
  [a-uri]
  (let [host (uri/host a-uri)
        robots-cache (:robots-cache pegasus.state/config)
        robots-txt (.get robots-cache host)
        parsed (robots/parse robots-txt)
        impolite? (:impolite? pegasus.state/config)]
    (or impolite?
        (robots/crawlable? robots-txt
                           (uri/path a-uri)
                           :user-agent
                           (:user-agent pegasus.state/config)))))

(defn not-visited
  [a-uri]
  (let [visited-cache (:visited-cache pegasus.state/config)]
    (not (.get visited-cache a-uri))))

(defn not-enqueued
  [a-uri]
  (let [to-visit-cache (:to-visit-cache pegasus.state/config)]
    (not (.get to-visit-cache a-uri))))

(defn default-filter
  "By default, we ignore robots.txt urls"
  [obj]
  (merge obj
         {:extracted
          (distinct
           (filter #(and %
                         (robots-filter %)
                         (not-visited %)
                         (not-enqueued %))
                   (:extracted obj)))}))

(defn close-wrtr
  []
  (let [wrtr (:writer @(:state pegasus.state/config))]
    (.close wrtr)))

(defn stop-program
  []
  (System/exit 0))

(def default-pipeline-config
  {:frontier default-frontier-fn
   :extractor default-extractor-fn
   :writer default-writer-fn
   :enqueue queue/enqueue-pipeline
   :update-state default-update-state
   :test-and-halt default-stop-check
   :filter default-filter
   :stop-sequence [close-wrtr stop-program]
   :pipeline [[:frontier s/Str 5]
              [:extractor {:url s/Str,
                           :body s/Str,
                           :time s/Int} 5]
              [:update-state {:url s/Str,
                              :body s/Str,
                              :time s/Int
                              :extracted [s/Str]} 5]
              [:filter {:url s/Str
                        :body s/Str
                        :time s/Int
                        :extracted [s/Str]} 5]
              [:writer {:url s/Str
                        :body s/Str
                        :time s/Int
                        :extracted [s/Str]} 5]
              [:enqueue {:url s/Str
                         :body s/Str
                         :time s/Int
                         :extracted [s/Str]} 5]
              [:test-and-halt s/Any 5]]})

(defn enforce-pipeline-check
  [a-config]
  (let [pipeline (:pipeline a-config)]
    (doseq [[component-name _] pipeline]
      (when-not (get a-config component-name)
        (throw
         (IllegalArgumentException.
          (str "Component " component-name " missing in your config.")))))))

(defn build-pipeline-config
  [user-config]
  (let [new-config (merge default-pipeline-config user-config)]

    (enforce-pipeline-check new-config)

    new-config))

(def default-options {:min-delay-ms 2000
                      :state (atom {:num-visited 0})})
