(ns gae-to-kafka-initial-import.io.local-file
  (:require [clojure.java.io :as io]))

(defn write-to [file objs]
  (with-open [wrtr (io/writer file)]
    (doseq [o objs]
      (.write wrtr (str o "\n")))))

(defn read-from [file process-fn]
  (with-open [rdr (io/reader file)]
    (process-fn (line-seq rdr))))