(ns pegasus.process
  ""
  (:require [clojure.core.async :as async]
            [clojure.repl :refer [pst]]
            [pegasus.utils :refer [with-config]]
            [schema.core :as s]
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn  error  fatal  report
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env log-env)]))

(defn add-transducer
  [in xf parallelism]
  (let [out (async/chan (async/buffer 2048)
                        identity
                        (fn [x]
                          (info x)
                          nil))]
    (async/pipeline-blocking parallelism out xf in)
    out))

(defn run-process
  [component process-schema in-chan parallelism crawl-config]
  (add-transducer in-chan
                  (comp (map #(try
                                (merge %
                                       {:input (with-config (:config %)
                                                 (->> %
                                                      :input
                                                      (s/validate process-schema)
                                                      ((get crawl-config component))))})
                                (catch Exception e
                                  (do (info component)
                                      (error e)
                                      (merge % {:input nil})))))
                        (filter :input))
                  parallelism))

(defn initialize-pipeline
  "A pipeline contains kws - fn-map
  contains a map from the kws to implementations.
  The components (typically) read from a
  channel and write to a channel.
  The first component is fixed as the component
  that speaks to a queue.
  The last component is the writer"
  [config]
  (info (:pipeline config))
  (let [pipeline (:pipeline config)

        init-chan (async/chan (async/buffer 1024))

        final-out-chan (reduce
                        (fn [last-out-channel [component component-schema parallelism]]
                          (info :current-component component)
 
                          (run-process component
                                       component-schema
                                       last-out-channel
                                       parallelism
                                       config))
                        init-chan
                        pipeline)]

    init-chan))
