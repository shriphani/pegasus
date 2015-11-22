(ns pegasus.defaults
  "Contains default components"
  (:require [clj-http.client :as client]
            [clj-time.core :as t]
            [clojure.pprint :refer [pprint]]
            [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri])
  (:import [clojure.lang PersistentQueue]
           [java.io StringReader]))

;; the in-memory queue holds a q
;;; in memory
(defn get-in-memory-queue
  []
  (atom PersistentQueue/EMPTY))

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
  (println "Frontier grabbing: " url)
  {:url  url
   :body (-> url
             client/get
             :body)
   :time (t/now)})

(defn default-extractor-fn
  "Default extractor extracts URLs from anchor tags in
  a page"
  [obj]
  (println "Extracting from: " (:url obj))
  (let [anchor-tags (-> obj
                        :body
                        StringReader.
                        html/html-resource
                        (html/select [:a]))
        
        url         (:url obj)
        
        extracted   {:extracted (map
                                 #(->> %
                                       :attrs
                                       :href
                                       (uri/resolve-uri url))
                                 anchor-tags)}]
    (merge obj extracted)))

(defn default-writer-fn
  "A writer writes to a writer or a stream.
  Default write is just a pprint"
  ([obj]
   (pprint obj))
  
  ([obj wrtr]
   (pprint obj wrtr)))

(defn default-enqueue
  [queue item]
  (println "Enqueueing: " item)
  (swap! queue conj item))

(defn default-visited-check
  [obj queue visited]
  (not
   (or (some #{(:url obj)}
             visited)
       (contains? queue
                  (:url obj)))))

(def default-options {:seeds []
                      :queue (get-in-memory-queue)
                      :dequeue  dequeue!
                      :frontier default-frontier-fn
                      :extractor default-extractor-fn
                      :writer default-writer-fn
                      :enqueue default-enqueue
                      :dequeue-fn identity
                      :pipeline [:dequeue-fn
                                 :frontier
                                 :extractor
                                 :writer]})
