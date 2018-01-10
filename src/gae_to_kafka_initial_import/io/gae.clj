(ns gae-to-kafka-initial-import.io.gae
  (:require
    [akvo.commons.gae :as gae]
    [akvo.commons.gae.query :as query]
    [clojure.tools.logging :as log]
    [clojure.java.io :refer (writer)]))

(defn datastore-spec [org-config]
  (assoc (select-keys org-config [:service-account-id :private-key-file])
    :hostname (str (:org-id org-config) ".appspot.com")
    :port 443))

(defn fetch-events
  [config f]
  (gae/with-datastore
    [ds (datastore-spec config)]
    (let [query (.prepare ds (query/query {:kind (:kind config)}))
          first-query-result (.asQueryResultList query (query/fetch-options {:limit 300}))]
      (loop [query-result first-query-result]
        (when-not (empty? query-result)
          (let [event-count (count query-result)
                _ (log/debugf "Read %s events from %s" event-count (:org-id config))
                process-result (f query-result)]
            (case process-result
              ::more (let [cursor (.getCursor query-result)
                           next-query-result (.asQueryResultList query
                                                                 (query/fetch-options {:limit        300
                                                                                       :start-cursor cursor}))]
                       (recur next-query-result))
              process-result)))))))

(comment

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