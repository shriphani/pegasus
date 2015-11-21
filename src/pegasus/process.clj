(ns pegasus.process
  "A process brings an async wrapper
  around a routine.
  A routine transforms a record or a map.
  A process reads from an in-channel (not necessarily)
  and writes to an out-channel (not necessarily)"
  (:require [clojure.core.async :as async]))

(defn run-queue-process
  "The queue process is special
  Runs first"
  [queue dequeue! out-chan]
  (async/go-loop []
   (let [next-url (dequeue! queue)]
     (when next-url
      (async/>! out-chan next-url)))))

(defn run-process
  [process-fn in-chan out-chan]
  (async/go-loop []
    (let [in-val  (async/<! in-chan)
          out-val (process-fn val)]
      (when in-val
       (async/>! out-chan out-val)))))

(defn run-write-process
  [process-fn in-chan]
  (async/go-loop []
    (let [in-val (async/<! in-chan)]
      (process-fn in-val))))

(defn initialize-data-structures
  "The critical data structures are:
  (i).   The queue
  (ii).  The set of visited URLs.

  Spin up both"
  [config]
  (let [seeds (:seeds config)
        queue (:queue config)]
    (doseq [seed seeds]
      (swap! queue conj seed))))

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
  (let [pipeline (:pipeline config)]
    (reduce
     (fn [channel [i component]]
       (cond (zero? i)
             (if (= component :dequeue)
               (let [out-chan (async/chan)]
                 (run-queue-process (:queue config)
                                    (:dequeue config)
                                    out-chan))
               (throw
                (Exception. "The first component must be the queue")))

             (= i (- (count pipeline) 1)) ;; pathetic.
             (let [in-chan  (async/chan)
                   component-fn (get pipeline component)]
               (run-write-process component-fn in-chan))
             
             :else
             (let [in-chan (async/chan)
                   out-chan (async/chan)
                   component-fn (get pipeline component)]
               (run-process component-fn
                            in-chan
                            out-chan))))
     nil
     (map-indexed vector pipeline))))
