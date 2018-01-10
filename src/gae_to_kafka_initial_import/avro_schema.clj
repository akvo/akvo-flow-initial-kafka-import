(ns gae-to-kafka-initial-import.avro-schema
  (:require
    [clojure.java.io :refer (reader)]
    clojure.walk
    [spec-provider.provider :as sp]
    [spec-provider.stats :as st]
    [cheshire.core :as cheshire]))

(defn can-be-null? [stats]
  (get-in stats [::st/pred-map nil?]))

(defn date? [o]
  (instance? java.util.Date o))

(defn predicates-including-date []
  (as-> spec-provider.stats/preds p
        (butlast p)
        (conj p date?)
        (conj p (complement (apply some-fn p)))))

(defn maybe-upgrade-to-enum [stats last-seen-name type]
  (if (and (seq (::st/distinct-values stats))
           (not (::st/hit-distinct-values-limit stats))
           (not (can-be-null? stats)))
    {:type {:type    "enum"
            :name    last-seen-name
            :symbols (::st/distinct-values stats)}}
    type))

(defn ^String uppercase-first
  [^CharSequence s]
  (let [s (.toString s)]
    (if (< (count s) 2)
      (.toUpperCase s)
      (str (.toUpperCase (subs s 0 1))
           (subs s 1)))))

(defn ->avro [stats last-seen-name]
  (let [without-nil? (dissoc (::st/pred-map stats) nil?)]
    (if (< 1 (count without-nil?))
      (throw (ex-info "dont know how to handle" {:i stats}))
      (let [guessed-type (condp = (first (keys without-nil?))
                           map? {:type   "record"
                                 :name   last-seen-name
                                 :fields (sort-by :name
                                                  (map (fn [[k v]]
                                                         (merge {:name k}
                                                                (->avro v (uppercase-first (name k)))))
                                                       (::st/keys (::st/map stats))))}
                           sequential? {:type {:type  "array"
                                               :items (->avro (::st/elements-coll stats) last-seen-name)}}
                           string? (maybe-upgrade-to-enum stats last-seen-name {:type "string"})
                           integer? {:type "long"}
                           double? {:type "double"}
                           boolean? {:type "boolean"}
                           date? {:type "long"}
                           nil nil
                           {:type "unknown" :value stats})]
        (if (can-be-null? stats)
          (update guessed-type :type (fn [t] (if t ["null" t] "null")))
          guessed-type)))))

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
