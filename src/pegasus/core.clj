(ns pegasus.core
  (:require [bigml.sketchy.bloom :as bloom]
            [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
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

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [config]
  (let [config*      (merge defaults/default-options config)
        
        [init-chan final-out-chan] (process/initialize-pipeline final-config)]

    ;; feed seeds
    (async/go
      (doseq [seed (:seeds final-config)]
        (let [initial-obj {:input seed :config final-config}]
          (async/>! init-chan initial-obj))))

    (println "Crawling begins")

    ;; crawl-loop
    (async/go-loop []
      (let [emitted         (async/<! final-out-chan)

            emitted-config  (:config emitted)
            urls-to-process (:input emitted)

            distinct-urls   (distinct urls-to-process)

            unvisited-urls  (filter
                             (fn [x]
                               (->> x
                                    (bloom/contains? @(:visited-bloom emitted-config))
                                    not))
                             distinct-urls)
            
            host-wise       (group-by uri/host
                                      unvisited-urls)]

        (insert-uris host-wise)
        )
      (recur))))

