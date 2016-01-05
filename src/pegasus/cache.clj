(ns pegasus.cache
  "Clojure wrapper around Ehcache"
  (:import [net.sf.ehcache Cache CacheManager Element]))

(defn make-cache
  [cache-name]
  (Cache. cache-name
          0 ; no limit
          true
          true
          0
          0
          true
          200))

(defn insert-into-cache
  [url cache]
  (let [element (Element. url url)]
    (.put cache element)))

(defn remove-from-cache
  [url cache]
  (.remove cache url))


(defn initialize-caches
  [config]
  (let [cache-mgr (.newInstance CacheManager)

        visited-cache (-> config
                          :visited-cache-name
                          make-cache)
        to-visit-cache (-> config
                           :to-visit-cache-name
                           make-cache)]
    {:cache-manager cache-mgr
     :visited-cache visited-cache
     :to-visit-cache to-visit-cache
     :update-cache (fn [obj]
                     (-> obj
                         :url
                         (remove-from-cache to-visit-cache))
                     (-> obj
                         :url
                         (insert-into-cache visited-cache))
                     obj)}))
