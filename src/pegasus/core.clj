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

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [config]
  (let [config*      (merge defaults/default-options config)
        bloom-filter (bloom/create (:estimated-crawl-size config*)
                                   (:false-positive-probability config*))

        with-bloom-config (merge (merge defaults/default-options
                                        config)
                                 {:visited-bloom (atom bloom-filter)})

        bloom-update-fn (fn [obj]
                          (do (swap! (:visited-bloom with-bloom-config)
                                     (fn [x]
                                       (defaults/default-bloom-update-fn x (:url obj))))
                              obj))

        final-config    (merge with-bloom-config
                               {:bloom-update bloom-update-fn})
        
        [init-chan final-out-chan] (process/initialize-pipeline final-config)]

    ;; feed seeds
    (async/go
      (doseq [seed (:seeds final-config)]
        (let [initial-obj {:input seed :config final-config}]
          (async/>! init-chan initial-obj))))

    (println "Crawling begins")

    ;; initialize bloom filters
    
    ;; crawl-loop
    (async/go-loop []
      (let [urls-to-process (:input (async/<! final-out-chan))
            host-wise       (group-by uri/host
                                      urls-to-process)]

        (doseq [[host host-uris] host-wise]
          
          (let [times     (get-time-ticks (count host-uris)
                                          (:min-delay final-config))
                
                [old new] (alternate-swap! (:host-last-ping-times final-config)
                                           (fn [x]
                                             (if (and (get x host)
                                                      (t/after? (get x host)
                                                                (t/now)))
                                               (merge-with t/plus x {host (last times)})
                                               (merge x {host (t/now)}))))

                chime-times (let [old-time (or (get old host) (t/now))]
                             (map
                              (fn [cur-time]
                                (if (t/after? (t/plus old-time cur-time) (t/now))
                                  (t/plus old-time
                                          cur-time)
                                  (t/plus (t/now)
                                          cur-time)))
                              times))


                chimes (chime-ch chime-times)]
            (async/go-loop [urls host-uris]
              (when (-> urls empty? not)
                (when-let [time (async/<! chimes)]
                  (async/>! init-chan {:config final-config
                                       :input  (first urls)})
                  (recur (rest urls))))))))
      (recur))))

