(ns pegasus.core
  (:require [chime :refer [chime-ch]]
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

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [config]
  (let [final-config (merge defaults/default-options
                            config)

        [init-chan final-out-chan] (process/initialize-pipeline final-config)]
    ;; feed seeds
    (async/go
      (doseq [seed (:seeds final-config)]
        (let [initial-obj {:input seed :config final-config}]
          (async/>! init-chan initial-obj))))

    (println "Crawling begins")
    
    ;; crawl-loop
    (async/go-loop []
      (let [urls-to-process (:input (async/<! final-out-chan))
            host-wise       (group-by uri/host
                                      urls-to-process)]

        (doseq [[host host-uris] host-wise]
          
          (let [times     (take
                           (count host-uris)
                           (map
                            (fn [a-time]
                              (t/seconds a-time))
                            (iterate #(+ (:min-delay final-config) %)
                                     (:min-delay final-config))))

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

