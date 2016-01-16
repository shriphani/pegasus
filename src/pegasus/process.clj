(ns pegasus.process
  "A process brings an async wrapper
  around a routine.
  A routine transforms a record or a map.
  A process reads from an in-channel (not necessarily)
  and writes to an out-channel (not necessarily)"
  (:require [clojure.core.async :as async]
            [clojure.repl :refer [pst]]
            [schema.core :as s]))

(defn add-transducer
  [in xf]
  (let [out (async/chan (async/buffer 2048)
                        identity
                        (fn [x]
                          (println x)
                          nil))]
    (async/pipeline-blocking 5 out xf in)
    out))

(defn run-process
  [process-fn process-schema in-chan]
  (add-transducer in-chan
                  (comp (filter :input)
                        (map #(try
                                (println %)
                                (merge %
                                       {:input (->> %
                                                    :input
                                                    (s/validate process-schema)
                                                    process-fn)})
                                (catch Exception e
                                  (do (println process-fn)
                                      (pst e)
                                      (merge % {:input nil}))))))))

(defn initialize-pipeline
  "A pipeline contains kws - fn-map
  contains a map from the kws to implementations.
  The components (typically) read from a
  channel and write to a channel.
  The first component is fixed as the component
  that speaks to a queue.
  The last component is the writer"
  [config]
  (println (:pipeline config))
  (let [pipeline (:pipeline config)

        init-chan (async/chan (async/buffer 1024))

        final-out-chan (reduce
                        (fn [last-out-channel [component component-schema]]
                          (println :current-component component)
                          (let [component-fn (get config component)] 
                            (run-process component-fn
                                         component-schema
                                         last-out-channel)))
                        init-chan
                        pipeline)]

    [init-chan final-out-chan]))
