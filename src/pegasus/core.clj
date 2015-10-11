(ns pegasus.core
  (:require [clojure.core.async :as async]
            [pegasus.frontier :as frontier]
            [pegasus.process :as process]))

;; Frontier - handles the to-visit queue.
;; Extractor - given a web-page, extract the next batch
;;; of links to download
;; Writer - Write a payload with other info to disk

;; Each process consumes from a channel and produces into
;;; a channel. A process obtains a record from its input
;;; channel, runs the associated routine on this record
;;; and sends in another record through the output channel
;;; Park these threads when they're waiting for a response

(defn frontier-process
  [queue-channel & channel]
  (if (empty? channel)
    (let [out-chan (async/chan)]
     (process/run-process frontier/frontier-process-fn
                          queue-channel
                          out-chan)
     out-chan)))

(defn writer-process
  "Demo writer process"
  [in-chan out-chan]
  (async/go-loop []
    (println "Fuck you bitch")
    (async/<! in-chan)))

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [options]
  (let [seeds (:seeds options)]
    (do (frontier/seed-queue seeds)
        (let [from-chan (frontier-process frontier/queue-chan)]
          (writer-process from-chan nil)))))
 
