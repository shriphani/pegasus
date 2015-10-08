(ns pegasus.frontier
  "A frontier contains the list of URLs to visit")

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
