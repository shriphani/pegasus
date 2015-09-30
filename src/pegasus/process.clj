(ns pegasus.process
  "A process brings an async wrapper
around a routine.
A routine transforms a record or a map.
A process reads from an in-channel (not necessarily)
and writes to an out-channel (not necessarily)"
  (:require [clojure.core.async :as async]))

(defn run-process
  [process-fn in-chan out-chan]
  (async/go-loop []
    (let [in-val  (async/<! in-chan)
          out-val (process-fn val)]
      (async/>! out-chan out-val))))
