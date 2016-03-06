(ns pegasus.utils
  "General utils"
  (:require [clojure.java.io :as io]
            [pegasus.state :as state])
  (:import [java.io FileInputStream InputStreamReader PushbackReader]
           [java.util.zip GZIPInputStream]))

(defn records
  [a-corpus-reader]
  (take-while
   identity
   (repeatedly
    (fn []
     (try (read a-corpus-reader)
          (catch Exception e nil))))))

(defn corpus-reader
  "A reader that supplies records from a corpus"
  [filename]
  (-> filename
      (FileInputStream.)
      (GZIPInputStream.)
      (InputStreamReader. "UTF-8")
      (PushbackReader.)))

(defmacro with-config
  ""
  [config & body]
  `(binding [pegasus.state/config ~config]
     ~@body))
