(ns pegasus.core
  (:require [clj-robots.core :as robots]
            [clj-time.core :as t]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [durable-queue :refer :all]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.defaults :as defaults]
            [pegasus.process :as process]
            [pegasus.queue :as queue]
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn  error  fatal  report
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env log-env)])
  (:import [pegasus.process PipelineComponentProtocol]))

(defn alternate-swap!
  "This version returns the swapped out value as well"
  [atom f & args]
  (loop []
    (let [old @atom
          new (apply f old args)]
      (if (compare-and-set! atom old new)
        [old new]  ; return value
        (recur)))))

(defn get-time-ticks
  "Generates a set of intervals
  (jodatime objects) separated by min-delay"
  [n min-delay]
  (take n
        (map
         t/seconds
         (iterate #(+ min-delay %) min-delay))))

(defn remove-fragments
  [a-uri]
  (uri/fragment a-uri nil))

(defn filter-uris
  "Given a list of URIs pick out an unseen distinct set.
  This is a helper for the enqueue op."
  [uris config]
  (let [to-visit-cache (:to-visit-cache config)
        visited-cache (:visited-cache config)

        no-fragments (map remove-fragments uris)
        unseen (filter
                (fn [a-uri]
                  (and (nil? (cache/lookup to-visit-cache
                                           a-uri))
                       (nil? (cache/lookup visited-cache
                                           a-uri))))
                no-fragments)]
    (distinct unseen)))

(defn handle-robots-url
  [robots-cache robots-url config]
  (let [body (-> robots-url
                 (defaults/get-request (:user-agent config))
                 :body)
        host (uri/host robots-url)]
    (if body
      (cache/miss robots-cache host body)
      (cache/miss robots-cache host "User-agent: *\nAllow: /"))))

(defn enforce-politeness
  [config]
  (when (and (-> config :user-agent not)
             (-> config :impolite? not))
    (throw
     (IllegalArgumentException. "Polite crawlers use a user-agent string."))))

(defn start-crawl
  [init-chan config]
  (info :starting-crawl)
  (let [seeds (:seeds config)]
    (async/go
      (doseq [seed seeds]
        (queue/enqueue-url seed
                           config)))))

(defn run-through-selectors
  [obj config]
  )

(defmacro extract
  [&body]
  `(reify
     process/PipelineComponentProtocol
     (initialize
       [this config]
       ~@body)

     (run
       [this obj config]
       (run-through-selectors obj
                              config))

     (clean
       [this config]
       nil)))

(defmacro from-xpath
  [xpath-query]
  (merge-with concat
              config
              {:xpath-queries [xpath-query]}))

(defmacro from-enlive-selector
  [selector]
  (merge-with concat
              config
              {:enlive-selectors [selector]}))

(defmacro from-enlive-selectors
  [selectors]
  (merge-with concat
              config
              {:enlive-selectors selectors}))

(defmacro from-xpaths
  [xpath-queries]
  (merge-with concat
              config
              {:xpath-queries xpath-queries}))

(defn crawl
  "Main entry point.
  Right now we have two ways of specifying seed URLs:
  :seed-list [seed1, seed2, ...]
  :seed-file /path/to/txt/file/with/seeds.
  Use just 1 :D.

  If you specify a destination (at :destination), all records
  are finally written there. Otherwise, we pprint
  to stdout.

  Config keys that we understand:
  :user-agent <user agent>"
  [config]

  (enforce-politeness config)
  
  (let [the-config (merge defaults/default-pipeline-config
                          config)
        initialized-config (process/initialize-pipeline the-config)
        init-chan (:init-chan initialized-config)]

    ;(defaults/config-logs final-config)
    
    (start-crawl init-chan initialized-config)
    initialized-config))

