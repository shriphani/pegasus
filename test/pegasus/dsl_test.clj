(ns pegasus.dsl-test
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [me.raynes.fs :as fs]
            [pegasus.core :refer :all]
            [pegasus.core-test :refer [mock-bodies
                                       *current-dir*
                                       job-dir-create-delete-fixture
                                       all-unique?]]
            [pegasus.defaults :as defaults]
            [pegasus.dsl :refer :all]
            [pegasus.utils :as utils]
            [taoensso.timbre :as timbre
             :refer (info)]
            [org.bovinegenius.exploding-fish :as uri]))

(use-fixtures :each job-dir-create-delete-fixture)

(deftest test-dsl
  (testing "Test the output of the DSL"
    (with-redefs [defaults/get-request (fn
                                         ([& args]
                                          (->> args
                                               first
                                               (get mock-bodies))))]
      (let [final-config (crawl {:seeds ["http://foo.com/1"]
                                 :impolite? true
                                 :user-agent "Hello!!!"
                                 :job-dir *current-dir*
                                 :corpus-size 5
                                 :min-delay-ms 0
                                 :extractor
                                 (defextractors
                                   (extract :at-selector [:a]
                                            :follow :href
                                            :when (fn [obj]
                                                    (-> obj
                                                        :url
                                                        uri/host
                                                        (= "foo.com")))))})]
        (loop []
          
          (let [stop (:stop?
                      @(:state final-config))]

            (if stop
              (do (is
                   (= (:num-visited
                       @(:state final-config))
                      5))
                  (is
                   (all-unique?
                    (io/file
                     (:corpus-dir final-config)))))
              (recur))))))))

(deftest test-dsl-regex
  (testing "Test the output of the DSL"
    (with-redefs [defaults/get-request (fn
                                         ([& args]
                                          (->> args
                                               first
                                               (get mock-bodies))))]
      (let [final-config (crawl {:seeds ["http://foo.com/1"]
                                 :impolite? true
                                 :user-agent "Hello!!!"
                                 :job-dir *current-dir*
                                 :corpus-size 5
                                 :min-delay-ms 0
                                 :extractor
                                 (defextractors
                                   (extract :at-selector [:a]
                                            :follow :href
                                            :with-regex #"foo.com"))})]
        (loop []
          
          (let [stop (:stop?
                      @(:state final-config))]

            (if stop
              (do (is
                   (= (:num-visited
                       @(:state final-config))
                      5))
                  (is
                   (all-unique?
                    (io/file
                     (:corpus-dir final-config)))))
              (recur))))))))
