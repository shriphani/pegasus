(ns pegasus.defaults
  "Contains default components"
  (:require [bigml.sketchy.bloom :as bloom]
            [clj-http.client :as client]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [me.raynes.fs :as fs]
            [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [schema.core :as s])
  (:import [clojure.lang PersistentQueue]
           [java.io StringReader]))

(declare config)

(defn get-request
  [url user-agent]
  (println :getting url)
  (client/get url {:throw-exceptions false
                   :socket-timeout 1000
                   :conn-timeout 1000
                   :headers {"User-Agent" user-agent}}))

(defn default-frontier-fn
  "The default frontier issues a GET request
  to the URL"
  [url]
  {:url  url
   :body (-> url
             (get-request (:user-agent config))
             :body)
   :time (-> (t/now)
             c/to-long)})

(defn default-extractor-fn
  "Default extractor extracts URLs from anchor tags in
  a page"
  [obj]
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
                          (= "nofollow"))
                     anchor-tags)

        uris        (map
                     #(->> %
                           :attrs
                           :href)
                     can-follow)

        clean-uris  (filter identity uris)
        
        extracted   {:extracted (map #(uri/resolve-uri url %)
                                     clean-uris)}]
    (merge obj extracted)))

(defn default-writer-fn
  "A writer writes to a writer or a stream.
  Default write is just a pprint"
  ([obj]
   (println "writer")
   obj)
  
  ([obj wrtr wrtr-lock]
   (locking wrtr-lock
     (pprint obj wrtr))
   obj))

(defn default-visited-check
  [obj queue visited]
  (not
   (or (some #{(:url obj)}
             visited)
       (contains? queue
                  (:url obj)))))

(defn default-stop-check
  "Stops at 20 pages."
  [config]
  (let [num-visited (:num-visited
                     @(:state config))]
    (<= 20 num-visited)))

(defn default-bloom-update-fn
  [bloom-filter url]
  (bloom/insert bloom-filter url))

(def default-location-config
  {:job-dir nil
   :corpus-dir "corpus"
   :struct-dir "data-structures"
   :logs-dir "logs"})

(defn make-absolute-path
  [base-dir a-path]
  (->> a-path
       (io/file base-dir)
       (.getAbsolutePath)))

(defn mkdir-if-not-exists
  [path]
  (let [file (io/file path)]
   (when-not (.exists file)
     (fs/mkdir (.getPath file)))))

(defn build-location-config
  [user-config]
  (let [relative-paths (merge default-location-config user-config)]
    ;; we are going to make these absolute paths
    (into
     {}
     (map
      (fn [[job-key rel-path]]
        (if (not= job-key :job-dir)
         (let [absolute-path (make-absolute-path
                              (:job-dir relative-paths)
                              rel-path)]
           (mkdir-if-not-exists absolute-path)
           [job-key absolute-path])))
      relative-paths))))


(def default-options {:seed nil
                      :frontier nil
                      :extractor default-extractor-fn
                      :writer default-writer-fn
                      :stop default-stop-check
                      :job-dir "/tmp" ; by-default data-structures sit in /tmp. Do change this :)
                      :struct-dir "data-structures"
                      :logs-dir "logs"
                      :corpus-dir "corpus"
                      :pipeline [[:frontier s/Str]
                                 [:update-cache {:url s/Str,
                                                 :body s/Str,
                                                 :time s/Int}] ; defined during the cache init phase
                                 [:extractor {:url s/Str,
                                              :body s/Str,
                                              :time s/Int}]
                                 [:writer {:url s/Str
                                           :body s/Str
                                           :time s/Int
                                           :extracted [s/Str]}]]
                      :host-last-ping-times (atom {})
                      :min-delay-ms 2000
                      :visited-cache-name "visited-cache"
                      :to-visit-cache-name "to-visit-cache"
                      :robots-cache-name "robots-cache"
                      :state (atom {:num-visited 0})})
