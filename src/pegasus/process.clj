(ns pegasus.process
  "A process brings an async wrapper
  around a routine.
  A routine transforms a record or a map.
  A process reads from an in-channel (not necessarily)
  and writes to an out-channel (not necessarily)"
  (:require [clojure.core.async :as async]))

(defn queue->chan
  ([queue dequeue-fn]
   (queue->chan queue dequeue-fn (async/chan)))

  ([queue dequeue-fn channel]
   (async/go-loop []
     (let [in-val (dequeue-fn queue)]
       (when in-val
         (async/>!! channel in-val))
       (recur)))
   channel))

(defn run-process
  [process-fn in-chan out-chan]
  (async/go-loop []
    (let [in-val  (async/<!! in-chan)
          out-val (process-fn in-val)]
      (println "Obj: " out-val)
      (when out-val
       (async/>!! out-chan out-val)))))

(defn run-write-process
  [process-fn in-chan]
  (async/go-loop []
    (let [in-val (async/<!! in-chan)]
      (println "Lol"
       (process-fn in-val)))))

(defn initialize-data-structures
  "The critical data structures are:
  (i).   The queue
  (ii).  The set of visited URLs.

  Spin up both"
  [config]
  (let [seeds (:seeds config)
        queue (:queue config)

        enqueue-fn (:enqueue config)]
    (doseq [seed seeds]
      (enqueue-fn queue seed))))

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
  (initialize-data-structures config)
  (let [pipeline (:pipeline config)

        dequeue-fn (:dequeue config)
        queue      (:queue config)
        
        queue-in-chan (queue->chan queue dequeue-fn)]

    (reduce
     (fn [last-out-channel component]
       (println :current-component component)
       (let [component-fn (get config component)]
         (let [out-chan (async/chan)]
           (run-process component-fn
                        last-out-channel
                        out-chan)
           out-chan)))
     queue-in-chan
     pipeline)))
