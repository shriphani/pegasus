(ns pegasus.frontier
  "A frontier contains the list of URLs to visit.
  This is going to be a process. The frontier
  dequeues a url, crawls it and continues."
  (:require [clj-http.client :as client]
            [clojure.core.async :as async]))

(def queue (atom clojure.lang.PersistentQueue/EMPTY))

(defn dequeue!
  []
  (loop []
    (let [q     @queue
          value (peek q)
          nq    (pop q)]
      (if (compare-and-set! queue q nq)
        value
        (recur)))))

(defn enqueue!
  [item]
  (swap! queue conj item))

(def queue-chan
  (let [channel (async/chan)]
    (async/go-loop []
      (async/>! channel (dequeue!)))))

(defn frontier-process-fn
  [url]
  (println url)
  url)

(defn seed-queue
  [seeds]
  (doseq [seed seeds]
    (enqueue! seed)))
