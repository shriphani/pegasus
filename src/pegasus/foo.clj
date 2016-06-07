(ns pegasus.foo
  (:require [org.bovinegenius.exploding-fish :as uri]
            [net.cgrand.enlive-html :as html]
            [pegasus.core :refer [crawl]])
  (:import (java.io StringReader)))

(defn crawl-sp-blog
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :extractor
          (fn [obj]
            ;; ensure that we only extract in domain
            (when (= "blog.shriphani.com"
                     (-> obj :url uri/host))

              (let [url (:url obj)
                    resource (-> obj
                                 :body
                                 (StringReader.)
                                 html/html-resource)

                    ;; extract the articles
                    articles (html/select resource
                                          [:article :header :h2 :a])

                    ;; the pagination links
                    pagination (html/select resource
                                            [:ul.pagination :a])

                    a-tags (concat articles pagination)

                    ;; resolve the URLs and stay within the same domain
                    links (filter
                           #(= (uri/host %)
                               "blog.shriphani.com")
                           (map
                            #(->> %
                                  :attrs
                                  :href
                                  (uri/resolve-uri (:url obj)))
                            a-tags))]

                ;; add extracted links to the supplied object
                (merge obj
                       {:extracted links}))))

          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"})) ;; store all crawl data in /tmp/sp-blog-corpus/

;; start crawling
