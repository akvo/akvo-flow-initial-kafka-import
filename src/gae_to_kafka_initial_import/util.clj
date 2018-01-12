(ns gae-to-kafka-initial-import.util
  (:require [gae-to-kafka-initial-import.gae-entity :as gae-entity]
            [clojure.tools.logging :as log]
            [camel-snake-kebab.core :refer [->camelCase]]
            [thdr.kfk.avro-bridge.core :as avro]
            [cheshire.core :as cheshire]
            [medley.core :as medley])
  (:import (org.apache.commons.lang3 SerializationUtils)
           (org.apache.avro.generic GenericDatumWriter GenericData$Record)
           (java.util Base64)
           (org.slf4j.bridge SLF4JBridgeHandler)
           (java.util.logging LogManager)
           (java.io ByteArrayOutputStream OutputStream)
           (java.nio.charset Charset)
           (org.apache.avro.io EncoderFactory)
           (com.google.appengine.api.datastore Entity)
           (org.apache.avro Schema)))

(set! *warn-on-reflection* true)

(.reset (LogManager/getLogManager))
(SLF4JBridgeHandler/removeHandlersForRootLogger)
(SLF4JBridgeHandler/install)

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
      (if (inst? v)
        (inst-ms v)
        v))
    m))

(defn clj->avro-generic-record [schema m]
  (avro/->java schema
               (dissoc m ::gae-entity/key)
               {:java-field-fn          (comp name ->camelCase)
                :ignore-unknown-fields? true}))

(defn ->avro-json-str [^Schema schema o]
  (let [bo (ByteArrayOutputStream.)
        json-enconder (.jsonEncoder (EncoderFactory/get) schema bo)]
    (.write (GenericDatumWriter. schema) o json-enconder)
    (.flush json-enconder)
    (.close bo)
    (String. (.toByteArray bo) (Charset/forName "UTF-8"))))

(defn transform-to [to schema]
  (let [all-steps [[:bytes #(and (string? %) (not (= \{ (first %)))) str->bytes]
                   [:gae-entity bytes? bytes->obj]
                   [:clj (partial instance? Entity) gae-entity->clj]
                   [:clj-date-as-long map? time->long]
                   [:generic-record map? (partial clj->avro-generic-record schema)]
                   [:json-str (partial instance? GenericData$Record) (partial ->avro-json-str schema)]
                   [:avro-json #(and (string? %) (= \{ (first %))) cheshire/parse-string]]
        steps-to-perform (medley/take-upto #(= to (first %)) all-steps)]
    (fn [x]
      (reduce (fn [v [_ pred transformation]]
                (if (pred v)
                  (transformation v)
                  v))
              x
              steps-to-perform))))