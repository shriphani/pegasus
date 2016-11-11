(ns pegasus.foo
  (:require [org.bovinegenius.exploding-fish :as uri]
            [net.cgrand.enlive-html :as html]
            [pegasus.core :refer [crawl]]
            [pegasus.process :as process])
  (:import (java.io StringReader)))

(defn crawl-sp-blog
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"})) ;; store all crawl data in /tmp/sp-blog-corpus/

(deftype EnliveExtractor []
  process/PipelineComponentProtocol
  
  (initialize
    [this config]
    config)
  
  (run
    [this obj config]
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

  (clean
    [this config]
    nil))

(defn crawl-sp-blog-custom-extractor
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :extractor (->EnliveExtractor)

          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"})) ;; store all crawl data in /tmp/sp-blog-corpus/

(deftype XpathExtractor []
  process/PipelineComponentProtocol
  
  (initialize
    [this config]
    config)
  
  (run
    [this obj config]
    (when (= "blog.shriphani.com"
             (-> obj :url uri/host))
      
      (let [url (:url obj)
            resource (try (-> obj
                              :body
                              xml->doc)
                          (catch Exception e nil))
            
            ;; extract the articles
            articles (map
                      :text
                      (try ($x "//item/link" resource)
                           (catch Exception e nil)))]
        
        ;; add extracted links to the supplied object
        (merge obj
               {:extracted articles}))))

  (clean
    [this config]
    nil))

(defn crawl-sp-blog-xpaths
  []
  (crawl {:seeds ["http://blog.shriphani.com/feeds/all.rss.xml"]
          :user-agent "Pegasus web crawler"
          :extractor (->XpathExtractor)
          
          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"}))

;; start crawling
