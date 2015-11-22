(defproject pegasus "0.1.0-SNAPSHOT"
  :description "A scaleable production-ready crawler in clojure"
  :url "http://github.com/shriphani/pegasus"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "2.0.0"]
                 [clj-time "0.11.0"]
                 [enlive "1.1.6"]
                 [org.bovinegenius/exploding-fish "0.3.4"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.mapdb/mapdb "2.0-beta8"]])
