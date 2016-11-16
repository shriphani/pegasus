(ns pegasus.foo
  (:require [org.bovinegenius.exploding-fish :as uri]
            [net.cgrand.enlive-html :as html]
            [pegasus.core :refer [crawl]]
            [pegasus.dsl :refer :all]
            [pegasus.process :as process])
  (:import (java.io StringReader)))

(defn crawl-sp-blog
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"})) ;; store all crawl data in /tmp/sp-blog-corpus/

(defn crawl-sp-blog-custom-extractor
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :extractor (defextractors
                       (extract :at-selector [:article :header :h2 :a]

                                :follow :href

                                :with-regex #"blog.shriphani.com")
                       
                       (extract :at-selector [:ul.pagination :a]

                                :follow :href
                                
                                :with-regex #"blog.shriphani.com"))
          
          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"})) ;; store all crawl data in /tmp/sp-blog-corpus/


