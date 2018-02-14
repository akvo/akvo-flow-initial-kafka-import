(ns gae-to-kafka-initial-import.avro-schema
  (:require
    [clojure.java.io :refer (reader)]
    clojure.walk
    clojure.string
    [spec-provider.stats :as st]))

(defn can-be-null? [stats parent-sample-count]
  (or
    (get-in stats [::st/pred-map nil?])
    (< (::st/sample-count stats) parent-sample-count)))

(defn predicates-including-date []
  (as-> st/preds p
        (butlast p)
        (conj p inst?)
        (conj p (complement (apply some-fn p)))))

(defn examples [stats]
  (seq (filter some? (::st/distinct-values stats))))

(defn maybe-upgrade-to-enum [stats last-seen-name type parent-sample-count]
  (if (and (examples stats)
           (every? string? (examples stats))
           (not (::st/hit-distinct-values-limit stats))
           (not (can-be-null? stats parent-sample-count)))
    {:type {:type    "enum"
            :name    last-seen-name
            :symbols (examples stats)}}
    type))

(defn ^String uppercase-first
  [^CharSequence s]
  (let [s (.toString s)]
    (if (< (count s) 2)
      (.toUpperCase s)
      (str (.toUpperCase (subs s 0 1))
           (subs s 1)))))

(defn example-doc [examples]
  (let [examples-without-nil (->> examples
                                  (filter some?)
                                  (map (fn [v] (if (= v "") "empty string" v)))
                                  (map (fn [v] (if (inst? v)
                                                 (str v " (" (inst-ms v) ")")
                                                 v)))
                                  (map (fn [v] (str "<" v ">"))))]
    (if (= 1 (count examples-without-nil))
      (str "It is always '" (first examples-without-nil) "'")
      (str "Examples: "
           (->> examples-without-nil
                (take 3)
                (clojure.string/join ", "))))))

(declare schema)

(defn type-for [last-seen-name stats [k v]]
  (let [avro-representation
        (condp = k

          map? {:type   "record"
                :name   last-seen-name
                :fields (sort-by :name
                                 (let [map-stats (::st/map stats)]
                                   (map (fn [[k v]]
                                          (merge {:name (name k)}
                                                 (schema v (uppercase-first (name k)) (::st/sample-count map-stats))))
                                        (::st/keys map-stats))))}

          sequential? {:type {:type  "array"
                              :items (schema (::st/elements-coll stats) last-seen-name)}}

          string? (maybe-upgrade-to-enum stats last-seen-name {:type "string"} (::st/sample-count stats))

          integer? {:type "long"}
          double? {:type "double"}
          boolean? {:type "boolean"}
          inst? {:type "long"}
          nil? {:type "null"}
          {:type "unknown" :value stats})]

    (cond-> avro-representation

            (::st/min v)
            (update :doc str "Range [" (::st/min v) "," (::st/max v) "]. ")

            (::st/min-length v)
            (update :doc str "Size [" (::st/min-length v) "," (::st/max-length v) "]. "))))

(defn nil-doc [stats parent-sample-count]
  (let [times-without-the-field (- parent-sample-count (::st/sample-count stats 0))
        times-the-field-is-nil (get-in stats [::st/pred-map nil? ::st/sample-count] 0)]
    (if (= (+ times-without-the-field times-the-field-is-nil) parent-sample-count)
      "Always nil"
      (str (format "Nil %.2f%%"
                   (* 100.0
                      (/ (+ times-without-the-field times-the-field-is-nil)
                         parent-sample-count)))))))

(defn move-null-to-first [types]
  (if (and (sequential? types)
           (some #{"null"} types))
    (cons "null" (remove #{"null"} types))
    types))

(defn add-null [types]
  (if (= "null" types)
    types
    (distinct (if (sequential? types)
                (cons "null" types)
                ["null" types]))))

(defn schema
  ([stats root-name]
   (schema stats root-name (::st/sample-count stats)))
  ([stats last-seen-name parent-sample-count]
   (let [all-types (map (fn [x]
                          (type-for last-seen-name stats x))
                        (::st/pred-map stats))
         avro-representation (condp = (count all-types)
                               0 (throw (ex-info "WEE@@@???" {:i stats}))
                               1 (first all-types)
                               {:type (mapv (fn [type] (if (= "record" (:type type))
                                                         type
                                                         (:type type))) all-types)
                                :doc  (clojure.string/join " OR " (remove nil? (map :doc all-types)))})]
     (cond-> avro-representation

             true
             (update :type move-null-to-first)

             (can-be-null? stats parent-sample-count)
             (-> (update :type add-null)
                 (update :doc (fn [doc] (str doc (nil-doc stats parent-sample-count)))))

             (examples stats)
             (update :doc (fn [doc] (str (example-doc (examples stats)) ". " doc)))))))

(defn collect-stats [coll]
  (binding [st/preds (predicates-including-date)]
    (st/collect
      coll
      {::st/distinct-limit 20})))

(defn avro-schema [namespace type-name coll]
  (let [stats (collect-stats coll)]
    (assoc (schema stats type-name) :namespace namespace)))
