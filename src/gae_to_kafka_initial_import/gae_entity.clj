(ns gae-to-kafka-initial-import.gae-entity
  "Adapted from https://github.com/migae/datastore/blob/master/src/clj/migae/datastore/types/entity_map.clj"
  (:require [clojure.edn :as edn])
  (:import (com.google.appengine.api.datastore Key EmbeddedEntity Email Link Entity)
           (java.util Date Collections$UnmodifiableMap Collection)))

(declare ds-to-clj-coll ds-to-clj)

(defn- embedded-entity->map
  [ee]
  (let [props (.getProperties ee)
        emap (into {} (for [[k v] props]
                        (do
                          (let [prop (keyword k)
                                val (ds-to-clj v)]
                            {prop val}))))]
    (assoc emap ::key (.. ee getKey getId))))

(defn ds-to-clj
  [v]
  (let [val (cond (integer? v) v
                  (string? v) (str v)
                  (= (class v) Double) v
                  (= (class v) Boolean) v
                  (= (class v) Date) v

                  (= (class v) Collections$UnmodifiableMap)
                  (let [props v]
                    (into {} (for [[k v] props]
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 {prop val}))))

                  (instance? Collection v) (ds-to-clj-coll v)
                  (= (type v) Link) (.toString v)

                  (= (type v) Email) (.getEmail v)
                  (= (type v) EmbeddedEntity)
                  (embedded-entity->map v)

                  (= (type v) Entity)
                  (embedded-entity->map v)

                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       (str \' (.getName v))))
                  (nil? v) nil
                  :else (throw (RuntimeException. (str "HELP: ds-to-clj else [" v "][" (class v) "]" (nil? v)))))]
    val))

(defn- ds-to-clj-coll
  "Type conversion: java datastore to clojure"
  [coll]
  (cond
    (= (type coll) java.util.ArrayList) (into '() (for [item coll]
                                                    (ds-to-clj item)))
    (= (type coll) java.util.HashSet)  (into #{} (for [item coll]
                                                   (ds-to-clj item)))
    (= (type coll) java.util.Vector)  (into [] (for [item coll]
                                                 (ds-to-clj item)))
    :else (throw (ex-info "???" {:c coll}))))
