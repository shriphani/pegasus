(ns pegasus.core
  (:require [bigml.sketchy.bloom :as bloom]
            [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
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

(defn setup-data-structures
  "Sets up ehcache."
  [])

(defn setup-loop
  [init-chan final-chan]
  (let [retrieved (async/go
                   (async/<! final-chan))]
    (println :retrieved retrieved)))

(defn crawl-loop
  "Sets up a crawl-job's loop"
  [config]
  (let [final-config (merge defaults/default-options config)]

    (setup-jobdir (:job-dir final-config)
                  (:logs-dir final-config)
                  (:struct-dir final-config))
    

    (setup-data-structures)

    (let [[init-chan final-chan]
          (process/initialize-pipeline final-config)]

      (setup-loop init-chan final-chan)

      ;; all systems are a go!
      (async/go
       (async/>! init-chan {:input  (:seed final-config)
                            :config final-config})))))

