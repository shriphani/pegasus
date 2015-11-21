(ns pegasus.core
  (:require [pegasus.defaults :as defaults]
            [pegasus.process :as process]))

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [options]
  (let [final-options (merge defaults/default-options
                             options)]
    final-options))
 
