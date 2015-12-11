(ns pegasus.core
  (:require [chime :refer [chime-ch]]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.defaults :as defaults]
            [pegasus.process :as process]))

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [config]
  (let [final-config (merge defaults/default-options
                            config)

        [init-chan final-out-chan] (process/initialize-pipeline final-config)]
    ;; feed seeds
    (async/go []
      (doseq [seed (:seeds final-config)]
        (async/>! init-chan seed)))

    (println "Crawling begins")
    
    ;; 
    (async/go-loop []
      (let [items (async/<! final-out-chan)
            host-wise (group-by uri/host items)]

        (doseq [[host host-uris] host-wise]
          (let [chimes (chime-ch
                        (take (count host-uris)
                              (map
                               (fn [sec]
                                 (-> sec t/seconds t/from-now))
                               (iterate #(+ 5 %) 0))))]
            (async/go-loop [urls host-uris]
              (when (-> urls empty? not)
                (when-let [time (async/<! chimes)]
                  (async/>! init-chan (first urls))
                  (recur (rest urls))))))))
      (recur))))

