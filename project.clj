(defproject pegasus "0.7.0"
  :description "A scaleable production-ready crawler in clojure"
  :url "http://github.com/shriphani/pegasus"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "2.1.0"]
                 [clj-named-leveldb "0.1.0"]
                 [clj-robots "0.6.0"]
                 [clj-time "0.11.0"]
                 [com.taoensso/timbre "4.3.1"]
                 [enlive "1.1.6"]
                 [factual/clj-leveldb "0.1.1"]
                 [fort-knox "0.4.0"]
                 [com.github.kyleburton/clj-xpath "1.4.5"]
                 [factual/durable-queue "0.1.5"]
                 [me.raynes/fs "1.4.6"]
                 [org.bovinegenius/exploding-fish "0.3.4"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [prismatic/schema "1.1.0"]
                 [slingshot "0.12.2"]]
  :plugins [[lein-ancient "0.6.8"]])
