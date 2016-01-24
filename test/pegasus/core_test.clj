(ns pegasus.core-test
  (:require [clj-http.client :as client]
            [clojure.test :refer :all]
            [pegasus.core :refer :all]
            [pegasus.defaults :as defaults]))

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

(deftest test-uniques
  (testing "Does the crawl grab unique URIs only?"
    (with-redefs [defaults/get-request (fn [x y]
                                         (get mock-bodies x))]
      (let [final-config (crawl {:seeds ["http://foo.com/1"]
                                 :impolite? true
                                 :user-agent "Hello!!!"
                                 :job-dir "/tmp/test-crawl"
                                 :corpus-size 5})]
        (loop []
          
          (let [stop (:stop?
                      @(:state final-config))]

            (if stop
              (is
               (= (:num-visited
                   @(:state final-config))
                  5))
              (recur))))))))
