(ns pegasus.extractor
  "Extractor takes a webpage and produces
  the next set of links"
  (:require [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri])
  (:import [java.io StringReader]))

(defn extract
  [page-body url]
  (let [anchor-tags (-> page-body
                        (StringReader.)
                        html/html-resource
                        (html/select [:a]))]
    (map
     #(->> %
           :attrs
           :href
           (uri/resolve-uri url))
     anchor-tags)))

