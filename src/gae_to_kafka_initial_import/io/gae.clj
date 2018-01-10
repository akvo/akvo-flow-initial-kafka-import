(ns gae-to-kafka-initial-import.io.gae
  (:require
    [akvo.commons.gae :as gae]
    [akvo.commons.gae.query :as query]
    [clojure.java.io :refer (writer)]))

(defn datastore-spec [org-config]
  (assoc (select-keys org-config [:service-account-id :private-key-file])
    :hostname (str (:org-id org-config) ".appspot.com")
    :port 443))

(defn fetch-events
  [config process-fn]
  (gae/with-datastore [ds (datastore-spec config)]
    (let [query (.prepare ds (query/query {:kind (:kind config)}))
          batch-size 300
          more-results (fn more-results [query-result]
                         (lazy-seq
                           (let [cursor (.getCursor query-result)
                                 next-query-result (.asQueryResultList query
                                                                       (query/fetch-options {:limit        batch-size
                                                                                             :start-cursor cursor}))]
                             (concat next-query-result
                                     (more-results next-query-result)))))]
      (process-fn (lazy-seq
                    (let [query-result (.asQueryResultList query (query/fetch-options {:limit batch-size}))]
                      (when (seq query-result)
                        (concat query-result
                                (more-results query-result)))))))))



(comment

  (fetch-events {:service-account-id "sa-akvoflowsandbox@akvoflowsandbox.iam.gserviceaccount.com"
                 :org-id             "akvoflowsandbox"
                 :kind               "SurveyInstance"
                 :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox/akvoflowsandbox.p12"}
                (fn [x] (gae-to-kafka-initial-import.io.local-file/write-as-serialized "foo.dat" (take 10 x))))

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