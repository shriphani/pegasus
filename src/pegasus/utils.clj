(ns pegasus.utils
  "General utils"
  (:require [clojure.java.io :as io])
  (:import [java.io PushbackReader]))

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
      io/reader
      (PushbackReader.)))
