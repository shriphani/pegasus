(ns pegasus.core
  (:require [bigml.sketchy.bloom :as bloom]
            [chime :refer [chime-ch]]
            [clj-robots.core :as robots]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [durable-queue :refer :all]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.cache :as cache]
            [pegasus.defaults :as defaults]
            [pegasus.process :as process]))

(defn alternate-swap!
  "This version returns the swapped out value as well"
  [atom f & args]
  (loop []
    (let [old @atom
          new (apply f old args)]
      (if (compare-and-set! atom old new)
        [old new]  ; return value
        (recur)))))

(defn get-time-ticks
  "Generates a set of intervals
  (jodatime objects) separated by min-delay"
  [n min-delay]
  (take n
        (map
         t/seconds
         (iterate #(+ min-delay %) min-delay))))

(defn setup-caches
  "Sets up ehcache."
  [config]
  (cache/initialize-caches config))

(defn remove-fragments
  [a-uri]
  (uri/fragment a-uri nil))

(defn filter-uris
  "Given a list of URIs pick out an unseen distinct set.
  This is a helper for the enqueue op."
  [uris config]
  (let [to-visit-cache (:to-visit-cache config)
        visited-cache (:visited-cache config)

        no-fragments (map remove-fragments uris)
        unseen (filter
                (fn [a-uri]
                  (and (nil? (.get to-visit-cache a-uri))
                       (nil? (.get visited-cache a-uri))))
                no-fragments)]
    (distinct unseen)))

(defn handle-robots-url
  [robots-cache robots-url config]
  (let [body (-> robots-url
                 (defaults/get-request (:user-agent config))
                 :body)
        host (uri/host robots-url)]
    (if body
      (.put robots-cache host body)
      (.put robots-cache host "User-agent: *\nAllow: /"))))

;; (defn setup-enqueue-loop
;;   "URLs come in out of final-chan and we put them
;;   in queues and stick them into init-chan."
;;   [init-chan final-chan config]
;;   (let [q (queues (:struct-dir config)
;;                   {})]
;;     (async/go-loop []
;;       (let [uris (-> final-chan
;;                      async/<!
;;                      :input
;;                      :extracted
;;                      (filter-uris config))
;;             uris-by-host (group-by uri/host uris)]
        
;;         (println :inserting uris)
;;         (doseq [[host host-uris] uris-by-host]
;;           (let [queue-name (keyword host)]
;;             (println queue-name)
;;             ;; first enqueue robots.txt if we've not seen
;;             ;; it before
;;             (when-not (.get (:hosts-visited-cache config) host)
;;               (println :enqueing-robots.txt)
;;               (let [robots-url (uri/resolve-uri (first host-uris)
;;                                                 "/robots.txt")]
;;                 (put! q queue-name robots-url)))
            
;;             (doseq [to-visit-uri host-uris]
;;               (put! q queue-name to-visit-uri)
;;               (.put (:to-visit-cache config)
;;                     to-visit-uri "1"))
            
;;             (when-not (.get (:hosts-visited-cache config) host)
              
;;              (.put (:hosts-visited-cache config) host "1") ; mark host as visited
             
;;              (async/go-loop []
;;                (async/<! (async/timeout 10000))     ; delay. FIXME: must come from config
;;                (let [next-uri-task (take! q queue-name)]
;;                  (println next-uri-task)
;;                  (let [next-url (deref next-uri-task)]
;;                    (if (= (uri/path next-url)
;;                           "robots.txt")
;;                      (handle-robots-url (:robots-cache config)
;;                                         next-url
;;                                         config)
;;                      (let [robots-txt (.get (:robots-cache config)
;;                                             host)
;;                            parsed-robots (robots/parse robots-txt)]
;;                        (when (robots/crawlable? parsed-robots
;;                                                 (uri/path next-url)
;;                                                 :user-agent
;;                                                 (:user-agent config))
;;                          (async/>! init-chan {:input next-url})))))
;;                  (deref next-uri-task))
;;                (recur)))))
;;        (recur)))))

;; (defn setup-stop-loop
;;   [config init-chan]
;;   (let [stop-check (:stop config)]
;;     (async/go-loop []
;;       (when (stop-check config)
;;         (do ;(println :stopping!)
;;             (async/close! init-chan)))
;;       (recur))))

;; (defn setup-crawl
;;   "Sets up a crawl-job's loops"
;;   [config]
;;   (let [user-config (merge defaults/default-options config)
;;         job-dir (:job-dir config)
;;         with-job-dir (merge user-config
;;                             {:logs-dir (.getAbsolutePath
;;                                         (io/file
;;                                          job-dir
;;                                          (:logs-dir config)))
;;                              :struct-dir (.getAbsolutePath
;;                                           (io/file
;;                                            job-dir
;;                                            (:struct-dir config)))
;;                              :corpus-dir (.getAbsolutePath
;;                                           (io/file
;;                                            job-dir
;;                                            (:corpus-dir config)))})
        
;;         with-cache-config (merge user-config
;;                                  (setup-caches with-job-dir))]
    
;;     (setup-jobdir (:job-dir with-cache-config)
;;                   (:logs-dir with-cache-config)
;;                   (:struct-dir with-cache-config)
;;                   (:corpus-dir with-cache-config))
    
;;     (let [[init-chan final-chan]
;;           (process/initialize-pipeline with-cache-config)
          
;;           final-config (merge with-cache-config
;;                               {:init-chan init-chan
;;                                :final-chan final-chan})]
      
;;       (setup-enqueue-loop init-chan final-chan final-config)
      
;;       (setup-stop-loop final-config init-chan)

;;       [init-chan final-chan final-config])))

(defn enforce-politeness
  [config]
  (when (and (-> config :user-agent not)
             (-> config :impolite? not))
    (throw
     (IllegalArgumentException. "Polite crawlers use a user-agent string."))))

(defn start-crawl
  [init-chan config]
  (println :starting-crawl)
  (let [seeds (:seeds config)]
    (async/go
      (doseq [seed seeds]
        (async/>! init-chan {:input  seed
                             :config config})))))

(defn crawl
  "Main entry point.
  Right now we have two ways of specifying seed URLs:
  :seed-list [seed1, seed2, ...]
  :seed-file /path/to/txt/file/with/seeds.
  Use just 1 :D.

  If you specify a destination (at :destination), all records
  are finally written there. Otherwise, we pprint
  to stdout.

  Config keys that we understand:
  :user-agent <user agent>"
  [config]

  (enforce-politeness config)

  (let [final-config* (-> config
                          defaults/add-location-config     ;; sets up the job directory
                          defaults/add-structs-config      ;; sets up caches
                          defaults/build-pipeline-config)  ;; builds a pipeline
        
        final-config  (merge defaults/default-options final-config*)

        init-chan     (process/initialize-pipeline final-config)]

    (start-crawl init-chan final-config)
    config))

