(ns pegasus.core
  (:require [clojure.core.async :as async]
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
      (let [items (async/<! final-out-chan)]
        (doseq [item items]
          (async/>! init-chan item)))
      (recur))))

