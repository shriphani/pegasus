(ns pegasus.cache
  "A simple cache using LMDB"
  (:require [clj-lmdb.simple :as lmdb]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [pegasus.utils :as utils]
            [taoensso.timbre :as timbre
             :refer (log debug info)]))

(defn add-to-cache
  [item cache]
  (lmdb/put! cache
             item
             "1"))

(defn remove-from-cache
  [item cache]
  (lmdb/delete! cache
                item))

(defn create-cache-dirs
  [cache-dir]
  (utils/mkdir-if-not-exists cache-dir))

(defn initialize-caches
  [config]

  ;; create cache directories
  (-> config
      :struct-dir
      create-cache-dirs)
  
  (let [visited-cache  (lmdb/make-named-db (:struct-dir config)
                                           "visited")
        to-visit-cache (lmdb/make-named-db (:struct-dir config)
                                           "to-visit")
        robots-cache   (lmdb/make-named-db (:struct-dir config)
                                           "robots")
        hosts-cache    (lmdb/make-named-db (:struct-dir config)
                                           "hosts")]
    
    {:to-visit-cache to-visit-cache
     :visited-cache visited-cache
     :hosts-visited-cache hosts-cache
     :robots-cache robots-cache}))

