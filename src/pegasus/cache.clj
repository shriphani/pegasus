(ns pegasus.cache
  "Clojure wrapper around JCS"
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import [org.apache.commons.jcs JCS]
           [org.apache.commons.jcs.engine.control CompositeCacheManager]
           [java.io ByteArrayInputStream]
           [java.nio.charset StandardCharsets]
           [java.util Properties]))

(def props (Properties.))
(def orig-config (-> "cache.ccf"
                     io/resource
                     slurp))

(defn remove-from-cache
  [item cache]
  (.remove cache item))

(defn add-to-cache
  [item cache]
  (.put cache item "1"))

(defn initialize-caches
  [config]
  (let [updated-config (string/replace orig-config #"PATH" (:struct-dir config))
        visited-cache (JCS/getInstance "visited")
        to-visit-cache (JCS/getInstance "tovisit")

        config-stream (ByteArrayInputStream.
                       (.getBytes updated-config StandardCharsets/UTF_8))]

;    (.setDiskPath visited-attrs (:struct-dir config))
;    (.setDiskPath to-visit-attrs (:struct-dir config))
    (.load props config-stream)
    (.configure (CompositeCacheManager/getUnconfiguredInstance)
                props)
    {:to-visit-cache to-visit-cache
     :visited-cache visited-cache
     :update-cache (fn [obj]
                     (-> obj
                         :url
                         (remove-from-cache to-visit-cache))

                     (-> obj
                         :url
                         (add-to-cache visited-cache))

                     obj)}))
