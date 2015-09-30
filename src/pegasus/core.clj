(ns pegasus.core
  (:require [clojure.core.async :as async :refer :all]))

;; Frontier - handles the to-visit queue.
;; Extractor - given a web-page, extract the next batch
;;; of links to download
;; Writer - Write a payload with other info to disk

;; Each process consumes from a channel and produces into
;;; a channel. A process obtains a record from its input
;;; channel, runs the associated routine on this record
;;; and sends in another record through the output channel
;;; Park these threads when they're waiting for a response
(def processes [:frontier :extractor :writer])

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [options]
  options)
