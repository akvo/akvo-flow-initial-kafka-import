(ns gae-to-kafka-initial-import.avro-schema
  (:require
    [clojure.java.io :refer (reader)]
    clojure.walk
    [spec-provider.provider :as sp]
    [spec-provider.stats :as st]
    [cheshire.core :as cheshire]))

(defn can-be-null? [stats parent-sample-count]
  (or
    (get-in stats [::st/pred-map nil?])
    (< (::st/sample-count stats) parent-sample-count)))

(defn predicates-including-date []
  (as-> spec-provider.stats/preds p
        (butlast p)
        (conj p inst?)
        (conj p (complement (apply some-fn p)))))

(defn examples [stats]
  (seq (filter some? (::st/distinct-values stats))))

(defn maybe-upgrade-to-enum [stats last-seen-name type parent-sample-count]
  (if (and (examples stats)
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
  (str "Examples: "
       (->> examples
            (filter some?)
            (map (fn [v] (if (= v "") "empty string" v)))
            (map (fn [v] (if (inst? v)
                           (str v " (" (inst-ms v) ")")
                           v)))
            (map (fn [v] (str "<" v ">")))
            (take 3)
            (clojure.string/join ", "))))

(defn ->avro
  ([stats root-name]
   (->avro stats root-name (::st/sample-count stats)))
  ([stats last-seen-name parent-sample-count]
   (let [without-nil?-predicate (dissoc (::st/pred-map stats) nil?)]
     (if (< 1 (count without-nil?-predicate))
       (throw (ex-info "dont know how to handle" {:i stats}))
       (let [[guessed-type avro-representation]
             (condp = (first (keys without-nil?-predicate))

               map? [map? {:type   "record"
                           :name   last-seen-name
                           :fields (sort-by :name
                                            (map (fn [[k v]]
                                                   (merge {:name k}
                                                          (->avro v (uppercase-first (name k)) (::st/sample-count stats))))
                                                 (::st/keys (::st/map stats))))}]

               sequential? [sequential? {:type {:type  "array"
                                                :items (->avro (::st/elements-coll stats) last-seen-name)}}]

               string? [string? (maybe-upgrade-to-enum stats last-seen-name {:type "string"} (::st/sample-count stats))]

               integer? [integer? {:type "long"}]
               double? [double? {:type "double"}]
               boolean? [boolean? {:type "boolean"}]
               inst? [inst? {:type "long"}]
               nil [nil nil]
               [:unknown {:type "unknown" :value stats}])
             guessed-type-stats (get without-nil?-predicate guessed-type)]

         (cond-> avro-representation

                 (can-be-null? stats parent-sample-count)
                 (update :type (fn [t] (if t ["null" t] "null")))

                 (examples stats)
                 (assoc :doc (example-doc (examples stats)))

                 (::st/min guessed-type-stats)
                 (update :doc str ". Range [" (::st/min guessed-type-stats) "," (::st/max guessed-type-stats) "]")))))))

(defn collect-stats [coll]
  (binding [spec-provider.stats/preds (predicates-including-date)]
    (spec-provider.stats/collect
      coll
      {::st/distinct-limit 20})))

(defn stats-for-file [file]
  (with-open [rdr (reader file)]
    (collect-stats
      (for [line (line-seq rdr)]
        (clojure.walk/keywordize-keys (read-string line))))))

(defn generate-avro [file type-name namespace]
  (let [stats (stats-for-file file)]
    (assoc (->avro stats type-name) :namespace namespace)))




(comment

  (generate-avro "akvoflowsandbox.SurveyedLocale.edn" "DataPoint" "org.akvo.flow")
  (spit "DataPoint.avsc" (cheshire/generate-string *1))

  (with-open [rdr (reader "akvoflowsandbox.SurveyedLocale.edn")]
    (sp/pprint-specs
      (sp/infer-specs
        (for [line (line-seq rdr)]
          (clojure.walk/keywordize-keys (read-string line)))
        :data/point
        {:spec-provider.stats/distinct-limit 100})
      *ns* 's))


  (def survey-group
    (into {}
          (map
            (juxt :akvo-unified-log.dan/key identity)
            (with-open [rdr (reader "akvoflowsandbox.SurveyGroup.edn")]
              (doall (for [line (line-seq rdr)]
                       (clojure.walk/keywordize-keys (read-string line))))))))

  (get survey-group 389005)

  (println (cheshire/generate-string (->avro (collect-stats (vals survey)) "Survey")))
  (->avro (collect-stats (vals survey)) "Survey")

  (def survey
    (reduce
      (fn [i v]
        (if (get i (:surveyGroupId v))
          (update-in i [(:surveyGroupId v) :form] conj v)
          i))
      survey-group
      (with-open [rdr (reader "akvoflowsandbox.Survey.edn")]
        (doall (for [line (line-seq rdr)]
                 (clojure.walk/keywordize-keys (read-string line))))))))
