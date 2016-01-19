(ns pegasus.queue
  "Enqueue and dequeue operations"
  (:require [clojure.core.async :as async]
            [durable-queue :refer :all]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.cache :as cache]
            [pegasus.state :as state]))

(defn setup-queue-worker
  [q q-name]
  (let [default-delay (:min-delay-ms state/config)
        init-chan (:init-chan state/config)]
    (async/go-loop []

      ;; a timeout first
      (async/<!
       (async/timeout
        (+ default-delay
           (rand-int 1000))))

      ;; draw a url and pass
      ;; it through the pipeline
      (let [task (take! q q-name)
            url (deref task)]
        (async/>! init-chan {:input url
                             :config state/config}))
      
      (recur))))

(defn enqueue-robots
  [a-url q visited-hosts-cache]
  (let [q-name (-> a-url uri/host keyword)
        robots-url (uri/resolve-uri a-url "/robots.txt")]
    (put! q q-name robots-url)))

(defn enqueue-url
  [url]
  (let [q-name (-> url uri/host keyword)
        struct-dir (:struct-dir state/config)
        q (queues struct-dir {:slab-size 1000})

        visited-hosts (:hosts-visited-cache state/config)]

    ;; before we first hit a host, we need to confirm
    ;; that robots.txt has been obtained.
    ;; if not, we enqueue robots.txt and start monitoring
    ;; that queue.
    (when-not (.get visited-hosts (uri/host url))
      (enqueue-robots url q visited-hosts)

      (setup-queue-worker q q-name))
    
    ;; insert this URL.
    (put! q q-name url)))

(defn enqueue-pipeline
  "A pipeline component that consumes urls
  from the supplied object, enqueues them
  and continues."
  [obj]
  (let [extracted-uris (:extracted obj)]
    (doseq [uri extracted-uris]
      (enqueue-url uri))
    obj))
