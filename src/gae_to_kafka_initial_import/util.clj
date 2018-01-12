(ns gae-to-kafka-initial-import.util
  (:require [gae-to-kafka-initial-import.gae-entity :as gae-entity]
            [clojure.tools.logging :as log])
  (:import (org.apache.commons.lang3 SerializationUtils)
           (org.apache.avro.generic GenericData)
           (java.util Base64)
           (org.slf4j.bridge SLF4JBridgeHandler)
           (java.util.logging LogManager)))

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

(defn valid-avro? [schema o]
  (.validate (GenericData/get) schema o))
