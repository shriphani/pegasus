(ns pegasus.core
  (:require [bigml.sketchy.bloom :as bloom]
            [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [durable-queue :refer :all]
            [me.raynes.fs :as fs]
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

(defn mkdir-if-not-exists
  [file]
  (when-not (.exists file)
    (fs/mkdir (.getPath file))))

(defn setup-jobdir
  "Creates subdirs for data structures, logs, corpora etc."
  [job-dir logs-dir struct-dir corpus-dir]
  (let [logs-dir-file (io/file job-dir logs-dir)
        struct-dir-file (io/file job-dir struct-dir)
        corpus-dir-file (io/file job-dir corpus-dir)]

    (mkdir-if-not-exists logs-dir-file)
    (mkdir-if-not-exists struct-dir-file)
    (mkdir-if-not-exists corpus-dir-file)))

(defn setup-caches
  "Sets up ehcache."
  [config]
  (cache/initialize-caches config))

(defn setup-enqueue-loop
  "Feed in extracted URIs."
  [init-chan final-chan config]
  (let [q (queues (:struct-dir config)
                  {})]
   (async/go-loop []
     (let [uris (-> final-chan
                    async/<!
                    :input)
           uris-by-host (group-by uri/host uris)]
       (doseq [host uris]
         (let [queue-name (keyword host)]
           (doseq [to-visit-uri uris]
             (put! q queue-name to-visit-uri))

           (when-not (.get (:hosts-visited-cache config) host)
             (.put (:hosts-visited-cache config) host "1") ; mark host as visited
             (async/go-loop []
               (async/<! (async/timeout 10000))     ; delay. FIXME: must come from config
               (let [next-uri-task (take! q queue-name)]
                 (println next-uri-task)
                 (async/>! init-chan {:input (deref next-uri-task)})
                 (deref next-uri-task))
               (recur)))))
       ;(recur)
       ))))

(defn crawl-loop
  "Sets up a crawl-job's loop"
  [config]
  (let [user-config (merge defaults/default-options config)
        
        with-cache-config (merge user-config
                                 (setup-caches user-config))]

    (setup-jobdir (:job-dir with-cache-config)
                  (:logs-dir with-cache-config)
                  (:struct-dir with-cache-config)
                  (:corpus-dir with-cache-config))

    (let [[init-chan final-chan]
          (process/initialize-pipeline with-cache-config)

          final-config (merge with-cache-config
                              {:init-chan init-chan
                               :final-chan final-chan})]

      (setup-enqueue-loop init-chan final-chan final-config)

      ;; all systems are a go!
      (async/go
       (async/>! init-chan {:input  (:seed final-config)
                            :config final-config})))))

