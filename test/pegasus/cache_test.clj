(ns pegasus.cache-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [pegasus.cache :as cache]))

(deftest test-initialize-cache
  (testing "Are the directories initialized?"
    (let [test-dir (fs/temp-dir "crawl")]
      (fs/mkdir test-dir)

      (let [final-config (cache/initialize-caches
                          {:struct-dir (.getAbsolutePath test-dir)})]

        (is
         (fs/exists?
          (:struct-dir final-config)))

        (is
         (:to-visit-cache final-config))

        (is
         (:visited-cache final-config))

        (is
         (:hosts-visited-cache final-config))

        (is
         (:robots-cache final-config))))))
