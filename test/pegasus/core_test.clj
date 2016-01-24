(ns pegasus.core-test
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [me.raynes.fs :as fs]
            [pegasus.core :refer :all]
            [pegasus.defaults :as defaults]
            [pegasus.utils :as utils]))

(def mock-bodies
  {"http://foo.com/1"
   {:url "http://foo.com/1"
    :body "<html>
 <body>
  <a href=\"2\">2</a>
  <a href=\"3\">3</a>
 </body>
</html>"}

   "http://foo.com/2"
   {:url "http://foo.com/2"
    :body "<html>
 <body>
  <a href=\"4\">4</a>
  <a href=\"5\">5</a>
 </body>
</html>"}

   "http://foo.com/3"
   {:url "http://foo.com/3"
    :body "<html>
 <body>
  <a href=\"6\">6</a>
 </body>
</html>"}

   "http://foo.com/4"
   {:url "http://foo.com/4"
    :body "<html>
 <body>
  <a href=\"5\">5</a>
 </body>
</html>"}

   "http://foo.com/5"
   {:url "http://foo.com/5"
    :body "<html>
 <body>
  <a href=\"6\">6</a>
 </body>
</html>"}

   "http://foo.com/6"
   {:url "http://foo.com/6"
    :body "<html>
 <body>
  <a href=\"7\">7</a>
 </body>
</html>"}})

(def default-test-dir "/tmp/test-crawl")

(defn remove-dir-fixture
  [f]
  (println "Running remove-dir fixture")
  (when (fs/exists? default-test-dir)
    (fs/delete-dir default-test-dir))
  (f)
  (when (fs/exists? default-test-dir)
    (fs/delete-dir default-test-dir)))

(use-fixtures :each remove-dir-fixture)

(defn all-unique?
  [corpus-dir]
  (let [corpus-file (io/file corpus-dir "corpus.clj")]
    (with-open [rdr (utils/corpus-reader corpus-file)]
      (let [urls
            (doall
             (map :url
                  (utils/records rdr)))]
        (= (count (set urls))
           (count urls))))))

(deftest test-stop-unique
  (testing "Does the crawl grab the correct number of docs? Are they unique?"
    (with-redefs [defaults/get-request (fn [x y]
                                         (get mock-bodies x))]
      (let [final-config (crawl {:seeds ["http://foo.com/1"]
                                 :impolite? true
                                 :user-agent "Hello!!!"
                                 :job-dir default-test-dir
                                 :corpus-size 5
                                 :min-delay-ms 0})]
        (loop []
          
          (let [stop (:stop?
                      @(:state final-config))]

            (if stop
              (is
               (= (:num-visited
                   @(:state final-config))
                  5))
              (recur)))))))

  (testing "Does the crawl grab the correct number of docs? Are they unique?"
    (with-redefs [defaults/get-request (fn [x y]
                                         (get mock-bodies x))]
      (let [final-config (crawl {:seeds ["http://foo.com/1"]
                                 :impolite? true
                                 :user-agent "Hello!!!"
                                 :job-dir default-test-dir
                                 :corpus-size 5
                                 :min-delay-ms 0})]
        (loop []
          
          (let [stop (:stop?
                      @(:state final-config))]

            (if stop
              (is
               (all-unique? (io/file (:corpus-dir final-config))))
              (recur))))))))
