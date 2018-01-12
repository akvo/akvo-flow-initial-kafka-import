(ns gae-to-kafka-initial-import.core
  (:require [gae-to-kafka-initial-import.io.local-file :as local-file]
            [gae-to-kafka-initial-import.util :as util]
            [thdr.kfk.avro-bridge.core :as avro]
            [gae-to-kafka-initial-import.io.kafka-rest-proxy :as kafka]
            [camel-snake-kebab.core :refer [->camelCase ->kebab-case]]
            [gae-to-kafka-initial-import.io.gae :as gae]
            [gae-to-kafka-initial-import.gae-entity :as gae-entity]))

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

(def str->clj (comp util/time->long
                    util/gae-entity->clj
                    util/bytes->obj
                    util/str->bytes))

(defn local-file->kafka-avro [binary-file topic schema]
  (local-file/read-from binary-file
                        (comp
                          (partial kafka/push-as-avro topic schema)
                          (partial map (comp
                                         (partial util/clj->avro-generic-record schema)
                                         str->clj))
                          util/log-progress)))

(defn check-local-file-against-schema [binary-file schema]
  (local-file/read-from binary-file
                        (comp
                          ;(juxt count )
                          first
                          (partial remove nil?)
                          (partial map (comp
                                         (fn [m]
                                           (try
                                             (if (util/valid-avro? schema
                                                                   (util/clj->avro-generic-record schema m))
                                               nil
                                               m)
                                             (catch Exception _ m)))
                                         str->clj))
                          util/log-progress)))
