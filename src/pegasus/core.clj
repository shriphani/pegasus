(ns pegasus.core
  (:require [clj-http.client :as client]
            [clj-time.core :as t]
            [clojure.pprint :refer [pprint]]
            [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.process :as process])
  (:import [clojure.lang PersistentQueue]
           [java.io StringReader]))

(def pipeline [:queue :frontier :extractor :writer :store])

;; the in-memory queue holds a q
;;; in memory
(def in-memory-queue (atom PersistentQueue/EMPTY))

;; stores are a collection of workers
;;; that pull in from a channel and
;;; work out of a channel

(defn dequeue!
  [queue]
  (loop []
    (let [q     @queue
          value (peek q)
          nq    (pop q)]
      (if (compare-and-set! queue q nq)
        value
        (recur)))))

(defn default-frontier-fn
  "The default frontier issues a GET request
  to the URL"
  [url]
  {:url  url
   :body (-> url
             client/get
             :body)
   :time (t/now)})

(defn default-extractor-fn
  "Default extractor extracts URLs from anchor tags in
  a page"
  [obj]
  (let [anchor-tags (-> obj
                        :body
                        StringReader.
                        html/html-resource
                        (html/select [:a]))
        
        url         (:url obj)
        
        extracted   (map
                     #(->> %
                           :attrs
                           :href
                           (uri/resolve-uri url))
                     anchor-tags)]
    (merge obj extracted)))

(defn default-writer-fn
  "A writer writes to a writer or a stream.
  Default write is just a pprint"
  ([obj]
   (pprint obj))
  
  ([obj wrtr]
   (pprint obj wrtr)))

(def default-options {:seeds []
                      :queue-fn nil
                      :frontier-fn default-frontier-fn
                      :extractor-fn default-extractor-fn
                      :writer-fn default-writer-fn})

(defn crawl
  "Main crawl method. Use this to spawn a new job"
  [options]
  (let [seeds (:seeds options)
        options (merge default-options options)]
    options))
 
