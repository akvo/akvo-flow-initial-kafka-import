(ns gae-to-kafka-initial-import.core
  (:require [gae-to-kafka-initial-import.io.local-file :as local-file]
            [gae-to-kafka-initial-import.util :as util]
            [gae-to-kafka-initial-import.io.kafka-rest-proxy :as kafka]
            [gae-to-kafka-initial-import.io.gae :as gae]))

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

(defn check-local-file-against-schema [binary-file schema]
  (let [transform (util/transform-to :json-str schema)]
    (frequencies
      (local-file/read-from binary-file
                            (comp
                              doall
                              (partial map (comp
                                             (fn [m]
                                               (try
                                                 (if (transform m)
                                                   [::ok nil]
                                                   (throw (RuntimeException. (str "This should not happen " m))))
                                                 (catch Exception e [::error (.getMessage e)])))))
                              util/log-progress)))))

(comment
  (def y (check-local-file-against-schema "akvoflowsandbox.SurveyedLocale.binary.txt" (org.akvo.flow.avro.DataPoint/getClassSchema)))
  (def x (check-local-file-against-schema "akvoflow-internal2.SurveyedLocale.binary.txt" (org.akvo.flow.avro.DataPoint/getClassSchema))))