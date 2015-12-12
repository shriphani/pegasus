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

(defn get-request
  [url]
  (client/get url {:throw-exceptions false
                   :socket-timeout 1000
                   :conn-timeout 1000}))

(defn default-frontier-fn
  "The default frontier issues a GET request
  to the URL"
  [url]
  (println "Frontier grabbing: " url)
  {:url  url
   :body (-> url
             get-request
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

        uris        (map
                     #(->> %
                           :attrs
                           :href)
                     anchor-tags)

        clean-uris  (filter identity uris)
        
        extracted   {:extracted (map #(uri/resolve-uri url %)
                                     clean-uris)}]
    (merge obj extracted)))

(defn default-writer-fn
  "A writer writes to a writer or a stream.
  Default write is just a pprint"
  ([obj]
                                        ;(pprint obj)
   (println "writer")
   (:extracted obj))
  
  ([obj wrtr]
                                        ;(pprint obj wrtr)
   (println "writer")
   (:extracted obj)))

(defn default-enqueue
  [queue item]
  (cond (map? item)
        (do (println "Enqueueing: " (:extracted item))
            (recur queue (:extracted item)))

        (coll? item)
        (do (println "Enqueueing: " item)
            (doseq [an-item item]
              (swap! queue conj an-item)))

        :else
        (do (println "Enqueueing: " item)
            (swap! queue conj item))))

(defn default-visited-check
  [obj queue visited]
  (not
   (or (some #{(:url obj)}
             visited)
       (contains? queue
                  (:url obj)))))

(def default-options (let [q (get-in-memory-queue)]
                      {:seeds []
                       :queue q
                       :dequeue  dequeue!
                       :frontier default-frontier-fn
                       :extractor default-extractor-fn
                       :writer default-writer-fn
                       :enqueue #(default-enqueue q %)
                       :pipeline [:frontier
                                  :extractor
                                        ;          :filters
                                  :writer]
                       :host-last-ping-times (atom {})
                       :min-delay 2}))
