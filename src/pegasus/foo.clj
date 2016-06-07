(ns pegasus.foo
  (:require [org.bovinegenius.exploding-fish :as uri]
            [net.cgrand.enlive-html :as html]
            [pegasus.core :refer [crawl]])
  (:import (java.io StringReader)))

(defn crawl-sp-blog
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"})) ;; store all crawl data in /tmp/sp-blog-corpus/

;; start crawling
