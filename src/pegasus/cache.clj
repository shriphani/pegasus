(ns pegasus.cache
  "A simple cache using LMDB"
  (:require [clj-lmdb.core :as lmdb]
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
  (utils/mkdir-if-not-exists
   (str cache-dir
        "/visited"))
  (utils/mkdir-if-not-exists
   (str cache-dir
        "/to-visit"))
  (utils/mkdir-if-not-exists
   (str cache-dir
        "/robots-txt"))
  (utils/mkdir-if-not-exists
   (str cache-dir
        "/hosts-visited")))

(defn initialize-caches
  [config]

  ;; create cache directories
  (-> config
      :struct-dir
      create-cache-dirs)
  
  (let [visited-dir  (str (:struct-dir config)
                          "/visited")
        to-visit-dir (str (:struct-dir config)
                          "/to-visit")
        robots-dir   (str (:struct-dir config)
                          "/robots-txt")
        hosts-dir    (str (:struct-dir config)
                          "/hosts-visited")

        visited-cache  (lmdb/make-db visited-dir)
        to-visit-cache (lmdb/make-db to-visit-dir)
        robots-cache   (lmdb/make-db robots-dir)
        hosts-cache    (lmdb/make-db hosts-dir)]
    
    {:to-visit-cache to-visit-cache
     :visited-cache visited-cache
     :hosts-visited-cache hosts-cache
     :robots-cache robots-cache}))

