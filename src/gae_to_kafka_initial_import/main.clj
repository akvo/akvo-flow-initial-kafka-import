(ns gae-to-kafka-initial-import.main
  (:require [gae-to-kafka-initial-import.io.gae :as gae]
            [gae-to-kafka-initial-import.io.local-file :as local-file]
            [gae-to-kafka-initial-import.io.kafka-rest-proxy :as kafka]
            [gae-to-kafka-initial-import.gae-entity :as gae-entity]
            [gae-to-kafka-initial-import.avro-schema :as avro-schema]
            [camel-snake-kebab.core :refer [->camelCase]]
            [clojure.tools.logging :as log])
  (:import (org.apache.commons.lang3 SerializationUtils)
           (java.util Base64)
           (org.apache.avro.generic GenericData)))

(defn log-progress
  ([s]
   (log-progress s 300))
  ([s n]
   (log-progress s n 0))
  ([s n so-far]
   (lazy-seq
     (when (zero? (mod so-far n))
       (log/info "progress: " so-far))
     (when (seq s)
       (cons (first s)
             (log-progress (rest s) n (inc so-far)))))))

(defn obj->bytes [o]
  (SerializationUtils/serialize o))

(defn bytes->obj [^bytes o]
  (SerializationUtils/deserialize o))

(defn bytes->str [bytes]
  (.encodeToString (Base64/getEncoder) bytes))

(defn str->bytes [^String s]
  (.decode (Base64/getDecoder) s))

(def gae-entity->clj gae-entity/ds-to-clj)

(defn time->long [m]
  (clojure.walk/postwalk
    (fn [v]
      (if (instance? java.util.Date v)
        (.getTime v)
        v))
    m))

(defn valid-avro? [schema o]
  (.validate (GenericData/get) schema o))

(defn gae->local-file [gae-config file]
  (gae/fetch-events gae-config
                    (comp (partial local-file/write-to file)
                          (partial map bytes->str)
                          (partial map obj->bytes)
                          log-progress)))

(defn local-file->kafka-binary [binary-file topic]
  (local-file/read-from binary-file
                        (comp
                          (partial kafka/push-as-binary topic)
                          log-progress)))


(defn local-file->kafka-avro [binary-file topic schema]
  (local-file/read-from binary-file
                        (comp
                          (partial kafka/push-as-avro topic schema)
                          (partial filter (partial valid-avro? schema))
                          (partial map (comp
                                         (fn [m]
                                           (avro/->java schema
                                                        (dissoc m ::gae-entity/key)
                                                        {:java-field-fn          (comp name ->camelCase)
                                                         :ignore-unknown-fields? true}))
                                         time->long
                                         gae-entity->clj
                                         bytes->obj
                                         str->bytes))
                          log-progress)))

(comment

  (gae->local-file {:service-account-id "sa-akvoflowsandbox@akvoflowsandbox.iam.gserviceaccount.com"
                    :org-id             "akvoflowsandbox"
                    :kind               "SurveyedLocale"
                    :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox/akvoflowsandbox.p12"}
                   "gae.strs")

  (local-file->kafka-binary "gae.strs" "binarytest")

  (local-file->kafka-avro "gae.strs" "nonbinarytest" (org.akvo.flow.avro.DataPoint/getClassSchema))

  (local-file/read-from "gae.strs"
                        (comp
                          (fn [stats] (gae-to-kafka-initial-import.avro-schema/->avro stats "Foo"))
                          gae-to-kafka-initial-import.avro-schema/collect-stats
                          (partial map (comp
                                         gae-entity->clj
                                         bytes->obj
                                         str->bytes))))


  (fetch-and-insert-new-events {:service-account-id "sa-akvoflowsandbox@akvoflowsandbox.iam.gserviceaccount.com"
                                :org-id             "akvoflowsandbox"
                                :kind               "SurveyInstance"
                                :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox/akvoflowsandbox.p12"})

  (fetch-and-insert-new-events {:service-account-id "sa-akvoflowsandbox@akvoflowsandbox.iam.gserviceaccount.com"
                                :org-id             "akvoflowsandbox"
                                :kind               "QuestionOption"
                                :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox/akvoflowsandbox.p12"})

  (fetch-and-insert-new-events {:service-account-id "sa-akvoflowsandbox@akvoflowsandbox.iam.gserviceaccount.com"
                                :org-id             "akvoflowsandbox"
                                :kind               "SurveyedLocale"
                                :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox/akvoflowsandbox.p12"})

  )
