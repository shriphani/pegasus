# pegasus

[![Circle CI](https://circleci.com/gh/shriphani/pegasus.svg?style=shield&circle-token=351e60b226583e6e24fece5d35f03fbb4f50d3bc)](https://circleci.com/gh/shriphani/pegasus)

<img src="pegasus_logo.png" align="middle" />

Pegasus is a highly-modular, durable and scalable crawler for clojure.

Parallelism is achieved with [`core.async`](https://clojure.github.io/core.async/).
Durability is achieved with [`durable-queue`](https://github.com/Factual/durable-queue) and [LMDB](https://symas.com/products/lightning-memory-mapped-database/).

A blog post on how pegasus works: [[link]](http://blog.shriphani.com/2016/01/25/pegasus-a-modular-durable-web-crawler-for-clojure/)

## Usage

Leiningen dependencies:

[![Clojars Project](https://img.shields.io/clojars/v/pegasus.svg)](https://clojars.org/pegasus)

A few example crawls:

This one crawls 20 docs from my blog (http://blog.shriphani.com).

URLs are extracted using `enlive` selectors.

```clojure
(ns your.namespace
  (:require [org.bovinegenius.exploding-fish :as uri]
            [net.cgrand.enlive-html :as html]
            [pegasus.core :refer [crawl]])
  (:import (java.io StringReader)))
			
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

(defn crawl-sp-blog
  []
  (crawl {:seeds ["http://blog.shriphani.com"]
          :user-agent "Pegasus web crawler"
          :extractor (->EnliveExtractor)

          :corpus-size 20 ;; crawl 20 documents
          :job-dir "/tmp/sp-blog-corpus"}))

(crawl-sp-blog)
```

This one uses XPath queries courtesy of `clj-xpath`.

Using XPaths:

```clojure
(ns your.namespace
  (:require [org.bovinegenius.exploding-fish :as uri]
            [net.cgrand.enlive-html :as html]
            [pegasus.core :refer [crawl]]
            [clj-xpath.core :refer [$x $x:text xml->doc]]))

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
(crawl-sp-blog-xpaths)          
```

## License

Copyright © 2015-2016 Shriphani Palakodety

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
