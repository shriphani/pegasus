(ns pegasus.defaults
  "Contains default components"
  (:require [clj-http.client :as client]
            [clj-robots.core :as robots]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.core.cache :as cache]
            [me.raynes.fs :as fs]
            [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.queue :as queue]
            [pegasus.cache :as pc]
            [pegasus.process :as process]
            [pegasus.utils :refer [mkdir-if-not-exists]]
            [schema.core :as s]
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn  error  fatal  report
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env log-env)]
            [taoensso.timbre.appenders.core :as appenders])
  (:import [clojure.lang PersistentQueue]
           [java.io BufferedWriter StringReader FileOutputStream OutputStreamWriter]
           [java.util.zip GZIPOutputStream]))

(defn get-request
  [url user-agent]
  (client/get url {:socket-timeout 1000
                   :conn-timeout 1000
                   :headers {"User-Agent" user-agent}}))

(deftype DefaultFrontierPipelineComponent []
  process/PipelineComponentProtocol

  (initialize [config]
    config)

  (run [url config]
    {:url  url
     :body (-> url
               (get-request (:user-agent config))
               :body)
     :time (-> (t/now)
               c/to-long)})

  (clean [config]
    nil))

(deftype DefaultExtractorPipelineComponent []
  process/PipelineComponentProtocol

  (initialize [config]
    config)
  
  (run [obj config]
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
          
          extracted   {:extracted (map #(uri/resolve-uri url %)
                                       clean-uris)}]
      (merge obj extracted)))

  (clean [config]
    nil))

(deftype DefaultWriterPipelineComponent []
  process/PipelineComponentProtocol

  (initialize
   [config]
   (when (-> config
             :state
             deref
             :writer
             nil?)
     (let [file-obj (-> config
                        :corpus-dir
                        (io/file "corpus.clj.gz"))
          wrtr (-> file-obj
                   io/output-stream
                   (GZIPOutputStream.)
                   (OutputStreamWriter. "UTF-8")
                   agent)]
       (swap! (:state config)
              merge
              {:writer wrtr})))
   config)

  (run
    [obj config]
    (let [gzip-out (-> config
                       :state
                       deref
                       :writer)

          write-fn (fn [wrtr s]
                     (.write wrtr
                             s)
                     wrtr)]
      (send-off gzip-out
                write-fn
                (str
                 (clojure.pprint/write obj :stream nil)
                 "\n"))
      obj))

  (clean [config]
         nil))

(defn default-visited-check
  [obj queue visited]
  (not
   (or (some #{(:url obj)}
             visited)
       (contains? queue
                  (:url obj)))))

(def default-location-config
  {:job-dir nil
   :corpus-dir "corpus"
   :struct-dir "data-structures"
   :logs-dir "logs"})

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
  (let [cache-config (pc/initialize-caches user-config)]
    (merge user-config cache-config)))

(deftype DefaultStatePipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [config]
    (-> config
        add-location-config
        add-structs-config))

  (run
    [obj config]
    (let [src-url (:url obj)
          
          to-visit-cache      (:to-visit-cache config)
          visited-cache       (:visited-cache config)
          hosts-visited-cache (:hosts-visited-cache config)

          extracted-uris (:extracted obj)]
      
      ;; cache updates
      (cache/miss to-visit-cache
                  src-url
                  "1")
      (cache/miss visited-cache
                  src-url
                  "1")
      (cache/miss hosts-visited-cache
                  (uri/host src-url)
                  "1")

      ;; return the object
      (swap! (:state config)
         (fn [x]
           (merge-with + x {:num-visited 1})))

      obj))

  (clean [config]
    nil))

(defn zero-enqueued?
  [q]
  (info :number-enqueued (queue/global-to-visit q))
  (zero?
   (queue/global-to-visit q)))

(defn default-stop-check
  "Stops at 100 documents. This
  function is part of the crawl
  pipeline but doesn't use any of the objects being
  passed through it; it only looks at
  the crawl-state."
  [obj config]
  (let [crawl-state (:state config)
        num-visited (:num-visited @crawl-state)
        corpus-size (:corpus-size config)

        q (:queue config)]
    (info :num-visited num-visited)
    (when (or (<= corpus-size num-visited)
              (zero-enqueued? (:queue config)))
      (let [init-chan (:init-chan config)
            stop-sequence (:stop-sequence config)]
        
        ;; do not accept any more URIs
        (async/close! init-chan)

        (info :stopping-crawl!)

        ;; any destructors needed are placed
        ;; here during the crawl phase
        (when stop-sequence
          (doseq [stop-fn stop-sequence]
            (stop-fn)))))))

(deftype DefaultStopPipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [config]
    config)

  (run
    [obj config]
    (default-stop-check obj config))

  (clean
    [obj]
    nil))

(defn robots-filter
  [a-uri config]
  (let [host (uri/host a-uri)
        robots-cache (:robots-cache config)
        robots-txt (cache/lookup robots-cache
                                 host)
        parsed (robots/parse robots-txt)
        impolite? (:impolite? config)]
    (or impolite?
        (robots/crawlable? robots-txt
                           (uri/path a-uri)
                           :user-agent
                           (:user-agent config)))))

(defn not-visited
  [a-uri config]
  (let [visited-cache (:visited-cache config)]
    (-> visited-cache
        (cache/lookup a-uri)
        not)))

(defn not-enqueued
  [a-uri config]
  (let [to-visit-cache (:to-visit-cache config)]
    (-> to-visit-cache
        (cache/lookup a-uri)
        not)))

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

(deftype DefaultFilterPipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [config]
    config)

  (run
    [obj config]
    (default-filter obj))

  (clean
    [config]
    nil))

(defn close-wrtr
  [config]
  (let [wrtr (:writer @(:state config))]
    (-> wrtr
        deref
        (.close))))

(defn mark-stop
  [config]
  (swap! (:state config)
         (fn [x]
           (merge x {:stop? true}))))

(deftype DefaultEnqueuePipelineComponent []
  process/PipelineComponentProtocol

  (initialize
    [config]
    (queue/build-queue-config config))

  (run
    [obj config]
    (queue/enqueue-pipeline obj config))

  (clean
    [config]
    nil))

(def default-pipeline-config
  {:frontier DefaultFrontierPipelineComponent
   :extractor DefaultExtractorPipelineComponent
   :writer DefaultWriterPipelineComponent
   :enqueue DefaultEnqueuePipelineComponent
   :update-state DefaultStatePipelineComponent
   :test-and-halt DefaultStopPipelineComponent
   :filter DefaultFilterPipelineComponent
   :stop-sequence [close-wrtr mark-stop]
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

(defn config-logs
  [config]
  (let [logs-dir (:logs-dir config)]
    (timbre/merge-config!
     {:appenders
      {:spit (appenders/spit-appender
              {:fname (.getAbsolutePath
                       (io/file logs-dir
                                "crawl.log"))})}})))

(def default-options {:min-delay-ms 2000
                      :state (atom {:num-visited 0})
                      :corpus-size 100})
