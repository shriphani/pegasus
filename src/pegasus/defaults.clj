(ns pegasus.defaults
  "Contains default components"
  (:require [bigml.sketchy.bloom :as bloom]
            [clj-http.client :as client]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.pprint :refer [pprint]]
            [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [schema.core :as s])
  (:import [clojure.lang PersistentQueue]
           [java.io StringReader]))

(defn get-request
  [url]
  (println :getting url)
  (client/get url {:throw-exceptions false
                   :socket-timeout 1000
                   :conn-timeout 1000}))

(defn default-frontier-fn
  "The default frontier issues a GET request
  to the URL"
  [url]
  {:url  url
   :body (-> url
             get-request
             :body)
   :time (-> (t/now)
             c/to-long)})

(defn default-extractor-fn
  "Default extractor extracts URLs from anchor tags in
  a page"
  [obj]
  (let [anchor-tags (-> obj
                        :body
                        (StringReader.)
                        html/html-resource
                        (html/select [:a]))
        
        url         (:url obj)

        can-follow  (filter
                     #(-> %
                          :attrs
                          :rel
                          (= "nofollow"))
                     anchor-tags)

        uris        (map
                     #(->> %
                           :attrs
                           :href)
                     can-follow)

        clean-uris  (filter identity uris)
        
        extracted   {:extracted (map #(uri/resolve-uri url %)
                                     clean-uris)}]
    (merge obj extracted)))

(defn default-writer-fn
  "A writer writes to a writer or a stream.
  Default write is just a pprint"
  ([obj]
   (println "writer")
   (:extracted obj))
  
  ([obj wrtr]
   (println "writer")
   (:extracted obj)))

(defn default-visited-check
  [obj queue visited]
  (not
   (or (some #{(:url obj)}
             visited)
       (contains? queue
                  (:url obj)))))

(defn default-stop-check
  "Stops at 20 pages."
  [config]
  (let [num-visited (:num-visited
                     @(:state config))]
    (<= 20 num-visited)))

(defn default-bloom-update-fn
  [bloom-filter url]
  (bloom/insert bloom-filter url))

(def default-options {:seed nil
                      :frontier default-frontier-fn
                      :extractor default-extractor-fn
                      :writer default-writer-fn
                      :stop default-stop-check
                      :job-dir "/tmp" ; by-default data-structures sit in /tmp. Do change this :)
                      :struct-dir "data-structures"
                      :logs-dir "logs"
                      :corpus-dir "corpus"
                      :pipeline [[:frontier s/Str]
                                 [:update-cache {:url s/Str,
                                                 :body s/Str,
                                                 :time s/Int}] ; defined during the cache init phase
                                 [:extractor {:url s/Str,
                                              :body s/Str,
                                              :time s/Int}]
                                 [:writer {:url s/Str
                                           :body s/Str
                                           :time s/Int
                                           :extracted [s/Str]}]]
                      :host-last-ping-times (atom {})
                      :min-delay 2
                      :visited-cache-name "visited-cache"
                      :to-visit-cache-name "to-visit-cache"
                      :robots-cache-name "robots-cache"
                      :state (atom {:num-visited 0})})
