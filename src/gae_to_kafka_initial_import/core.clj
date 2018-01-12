(ns gae-to-kafka-initial-import.core
  (:require [gae-to-kafka-initial-import.io.local-file :as local-file]
            [gae-to-kafka-initial-import.util :as util]
            [gae-to-kafka-initial-import.io.kafka-rest-proxy :as kafka]
            [gae-to-kafka-initial-import.io.gae :as gae]
            [clojure.tools.logging :as log]))

(defn gae->local-file [gae-config file]
  (gae/fetch-events gae-config
                    (comp (partial local-file/write-to file)
                          (partial map util/bytes->str)
                          (partial map util/obj->bytes)
                          util/log-progress)))

(defn local-file->kafka-binary [binary-file topic]
  (local-file/read-from binary-file
                        (comp
                          (partial kafka/push-as-binary topic)
                          util/log-progress)))

(defn local-file->kafka-avro [binary-file topic schema]
  (local-file/read-from binary-file
                        (comp
                          (partial kafka/push-as-avro topic schema)
                          util/log-progress)))

(defn errors [report]
  (when-not (empty? (dissoc report ::ok))
    (dissoc report ::ok)))

(defn log-report [report]
  (let [this-errors (errors report)]
    (log/info "There were" (::ok report) "ok records")
    (log/info "There were" (apply + (map second this-errors)) "errors records")
    (when this-errors
      (log/info "Most common errors:")
      (doseq [[msg times] (take 20 (sort-by last > this-errors))]
        (log/info "ERROR:" msg ", times" times)))))

(defn report-schema-compliance [binary-file schema]
  (let [transform (util/transform-to :json-str schema)]
    (local-file/read-from binary-file
                          (comp
                            frequencies
                            (partial pmap (fn [m]
                                            (try
                                              (if (transform m)
                                                ::ok
                                                (throw (RuntimeException. (str "This should not happen " m))))
                                              (catch Exception e (.getMessage e)))))
                            util/log-progress))))

(comment
  (def x (report-schema-compliance "akvoflow-internal2.SurveyedLocale.binary.txt" (org.akvo.flow.avro.DataPoint/getClassSchema)))
  (log-report x)
  (def y (report-schema-compliance "akvoflowsandbox.SurveyedLocale.binary.txt" (org.akvo.flow.avro.DataPoint/getClassSchema))))
