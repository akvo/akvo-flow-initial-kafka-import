(ns gae-to-kafka-initial-import.gae-entity
  (:require [clojure.edn :as edn])
  (:import (com.google.appengine.api.datastore Key EmbeddedEntity Email Link Entity)))

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
  ;; (log/trace "ds-to-clj:" v (type v) (class v))
  (let [val (cond (integer? v) v
                  (string? v) (str v)
                  (= (class v) java.lang.Double) v
                  (= (class v) java.lang.Boolean) v
                  (= (class v) java.util.Date) v

                  (= (class v) java.util.Collections$UnmodifiableMap)
                  (let [props v]
                    (into {} (for [[k v] props]
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 {prop val}))))

                  (instance? java.util.Collection v) (ds-to-clj-coll v)
                  (= (type v) Link) (.toString v)

                  (= (type v) Email) (.getEmail v)
                  (= (type v) EmbeddedEntity)
                  (embedded-entity->map v)

                  (= (type v) Entity)
                  (embedded-entity->map v)

                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       ;; (symbol (.getName v))))
                                       (str \' (.getName v))))
                  (nil? v) nil
                  :else (do
                          ;; (log/trace "HELP: ds-to-clj else " v (type v))
                          (throw (RuntimeException. (str "HELP: ds-to-clj else [" v "][" (class v) "]" (nil? v))))
                          (edn/read-string v)))]
    ;; (println "ds-to-clj result:" v val)
    val))

(defn ds-to-clj-coll
  "Type conversion: java datastore to clojure"
  [coll]
  ;; (log/trace "ds-to-clj-coll" coll (type coll))
  (cond
    (= (type coll) java.util.ArrayList) (into '() (for [item coll]
                                                    (ds-to-clj item)))
    (= (type coll) java.util.HashSet)  (into #{} (for [item coll]
                                                   (ds-to-clj item)))
    (= (type coll) java.util.Vector)  (into [] (for [item coll]
                                                 (ds-to-clj item)))
    :else (throw (ex-info "???" {:c coll}))
    ))
