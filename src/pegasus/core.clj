(ns pegasus.core
  (:require [bigml.sketchy.bloom :as bloom]
            [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
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

(defn setup-loop
  [init-chan final-chan]
  )

(defn crawl-loop
  "Sets up a crawl-job's loop"
  [config]
  (let [user-config (merge defaults/default-options config)
        
        with-cache-config (merge user-config
                                 (initialize-caches user-config))]

    (setup-jobdir (:job-dir with-cache-config)
                  (:logs-dir with-cache-config)
                  (:struct-dir with-cache-config)
                  (:corpus-dir with-cache-config))

    (let [[init-chan final-chan]
          (process/initialize-pipeline with-cache-config)

          final-config (merge with-cache-config
                              {:init-chan init-chan
                               :final-chan final-chan})]

      (setup-loop init-chan final-chan)

      ;; all systems are a go!
      (async/go
       (async/>! init-chan {:input  (:seed final-config)
                            :config final-config})))))

