(ns gae-to-kafka-initial-import.io.gae
  (:require
    [akvo.commons.gae :as gae]
    [akvo.commons.gae.query :as query]
    [clojure.java.io :refer (writer)]))

(defn datastore-spec [org-config]
  (assoc (select-keys org-config [:service-account-id :private-key-file])
    :hostname (str (:org-id org-config) ".appspot.com")
    :port 443))

(defn iter [next-thunk next-fn]
  (lazy-seq
    (let [result (next-thunk)]
      (when (seq result)
        (concat result
                (iter (partial next-fn result) next-fn))))))

(defn fetch-events
  [config process-fn]
  (gae/with-datastore [ds (datastore-spec config)]
    (let [query (.prepare ds (query/query {:kind (:kind config)}))
          batch-size 300]
      (process-fn
        (iter
          #(.asQueryResultList query (query/fetch-options {:limit batch-size}))
          (fn [query-result]
            (let [cursor (.getCursor query-result)]
              (.asQueryResultList query
                                  (query/fetch-options {:limit        batch-size
                                                        :start-cursor cursor})))))))))