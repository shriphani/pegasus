(ns pegasus.cache
  "A simple cache using LMDB"
  (:require [clj-lmdb.simple :as lmdb]
            [clj-leveldb :as leveldb]
            [clj-named-leveldb.core :as named-leveldb]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [fort-knox.leveldb :refer :all]
            [pegasus.utils :as utils]
            [taoensso.timbre :as timbre
             :refer (log debug info)]))

(defn create-cache-dirs
  [cache-dir]
  (utils/mkdir-if-not-exists cache-dir))

(defn initialize-caches
  [config]

  (let [two-tb    2147483648
        cache-dir (:struct-dir config)

        base-db   (leveldb/create-db cache-dir {})
        
        visited-db  (named-leveldb/make-named-db base-db
                                                 "visited")
        to-visit-db (named-leveldb/make-named-db base-db
                                                 "to-visit")
        robots-db   (named-leveldb/make-named-db base-db
                                                 "robots")
        hosts-db    (named-leveldb/make-named-db base-db
                                                 "hosts")
        
        visited-cache  (make-cache-from-db visited-db)
        to-visit-cache (make-cache-from-db to-visit-db)
        robots-cache   (make-cache-from-db robots-db)
        hosts-cache    (make-cache-from-db hosts-db)]
    
    ;; create cache directories
    (create-cache-dirs cache-dir)
    
    (merge
     config
     {:to-visit-cache      to-visit-cache
      :visited-cache       visited-cache
      :hosts-visited-cache hosts-cache
      :robots-cache        robots-cache})))
