(ns gae-to-kafka-initial-import.io.local-file
  (:import (java.io ObjectOutputStream FileOutputStream ObjectInputStream FileInputStream)
           (org.apache.commons.lang3 SerializationUtils)))


(defn write-as-serialized [file objs]
  (with-open [out (ObjectOutputStream. (FileOutputStream. file))]
    (doseq [o objs]
      (.writeObject out o))
    (.writeObject out nil)))

(defn read-from-serialized [file process-fn]
  (with-open [in (ObjectInputStream. (FileInputStream. file))]
    (process-fn
      (take-while some?
                  (repeatedly #(.readObject in))))))