(ns gae-to-kafka-initial-import.io.local-file
  (:import (java.io ObjectOutputStream FileOutputStream ObjectInputStream FileInputStream)
           (org.apache.commons.lang3 SerializationUtils)))


(defn write-to-file [file batch]
  (with-open [out (ObjectOutputStream. (FileOutputStream. file))]
    (batch (fn [events]
             (doseq [o events]
               (.writeObject out o))
             ::more))
    (.writeObject out nil)))

(defn read-from-file [file batch]
  (with-open [in (ObjectInputStream. (FileInputStream. file))]
    (batch
      (take-while some?
                  (repeatedly #(.readObject in))))))


(read-from-file "binary.dat"
                (fn [events] (println (take 10 (partition 10 events)))))