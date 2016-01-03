(ns pegasus.core
  (:require [bigml.sketchy.bloom :as bloom]
            [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [me.raynes.es.fs :as fs]
            [org.bovinegenius.exploding-fish :as uri]
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

(defn insert-uris
  [uris-grouped-by-host]
  ())

(defn setup-jobdir
  "Creates subdirs for data structures, logs etc."
  [job-dir logs-dir struct-dir]
  (let [logs-dir-file (io/file job-dir logs-dir)
        struct-dir-file (io/file job-dir struct-dir)]

    (when-not (.exists logs-dir-file)
      (fs/mkdir (.getPath logs-dir-file)))

    (when-not (.exists struct-dir-file)
      (fs/mkdir (.getPath struct-dir-file)))))

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [config]
  (let [final-config (merge defaults/default-options config)
        [init-chan final-out-chan] (process/initialize-pipeline final-config)]

    ;; job directories
    (setup-jobdir (:job-dir config)
                  (:logs-dir config)
                  (:struct-dir config))
    


    ;; set up data-structures
    )

