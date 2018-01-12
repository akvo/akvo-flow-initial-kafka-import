(ns gae-to-kafka-initial-import.scripts
  (:require [clojure.java.io :as io]
            [gae-to-kafka-initial-import.core :as initial-import]
            [medley.core :as medley]
            [camel-snake-kebab.core :refer [->camelCase ->kebab-case]]
            [clojure.tools.logging :as log])
  (:import (com.google.apphosting.utils.config AppEngineWebXmlReader)))

(defn instance-info
  [^java.io.File file]
  (let [config (-> file
                   .getAbsolutePath
                   (AppEngineWebXmlReader. "")
                   .readAppEngineWebXml)
        org-id (.getAppId config)]
    (medley/map-keys
      (fn [k] (-> k name ->kebab-case keyword))
      (into {:org-id           org-id
             :private-key-file (.getAbsolutePath (io/file (-> file .getParentFile) (str org-id ".p12")))}
            (.getSystemProperties config)))))

(defn find-all-enabled-tentants [dir]
  (->> dir
       io/file
       file-seq
       (filter (fn [f] (re-find #"appengine-web.xml$" (.getName f))))
       (map instance-info)
       (map #(select-keys % [:org-id :service-account-id :windshaft-maps :private-key-file]))
       (filter #(-> % :windshaft-maps (= "true")))
       (sort-by :org-id)))

(defn local-file-for [org-id kind]
  (str org-id "." kind ".binary.txt"))

(defn kafka-topic [org-id kind]
  (->
    (str "flow." org-id "." kind ".avro.v1")
    clojure.string/lower-case
    (clojure.string/replace #"[^a-z0-9\.]" "_")))

(defn download-from-gae [kind instances]
  (doseq [instance instances]
    (initial-import/gae->local-file
      (assoc instance :kind kind)
      (local-file-for (:org-id instance) kind))))

(defn validate-against-schema [schema kind instances]
  (doseq [instance instances]
    (let [report (initial-import/report-schema-compliance
                   (local-file-for (:org-id instance) kind)
                   schema)]
      (log/info "Schema report for" (:org-id instance) "kind" kind)
      (initial-import/log-report report)
      (when (initial-import/errors report)
        (throw (RuntimeException. (str "Schema errors. See logs")))))))

(defn push-to-kafka [schema kind instances]
  (doseq [instance instances]
    (initial-import/local-file->kafka-avro
      (local-file-for (:org-id instance) kind)
      (kafka-topic (:org-id instance) kind)
      schema)))

(comment

  (def kind "SurveyedLocale")
  (def instances (find-all-enabled-tentants "/Users/dlebrero/projects/akvo/akvo-flow-server-config/"))
  (def instances (find-all-enabled-tentants "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox"))
  (def schema (org.akvo.flow.avro.DataPoint/getClassSchema))

  (download-from-gae kind instances)
  (validate-against-schema schema kind instances)
  (push-to-kafka schema kind instances)

  )