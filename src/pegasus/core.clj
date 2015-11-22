(ns pegasus.core
  (:require [pegasus.defaults :as defaults]
            [pegasus.process :as process]))

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [config]
  (let [final-config (merge defaults/default-options
                            config)]
    (println "Final config:" final-config)
    (process/initialize-pipeline final-config)))
 
