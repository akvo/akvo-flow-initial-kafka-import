(ns gae-to-kafka-initial-import.main
  (:require [gae-to-kafka-initial-import.io.gae :as gae]
            [gae-to-kafka-initial-import.io.local-file :as local-file]
            [gae-to-kafka-initial-import.io.kafka-rest-proxy :as kafka]
            [gae-to-kafka-initial-import.gae-entity :as gae-entity]
            [gae-to-kafka-initial-import.util :as util]
            [thdr.kfk.avro-bridge.core :as avro]
            [gae-to-kafka-initial-import.avro-schema :as avro-schema]
            [camel-snake-kebab.core :refer [->camelCase ->kebab-case]]
            [clojure.tools.logging :as log]))

(comment

  (check-local-file-against-schema "gae-internal2.strs" (org.akvo.flow.avro.DataPoint/getClassSchema))
  (check-local-file-against-schema "gae.strs" (org.akvo.flow.avro.DataPoint/getClassSchema))

  (gae->local-file {:service-account-id "sa-akvoflow-internal2@akvoflow-internal2.iam.gserviceaccount.com"
                    :org-id             "akvoflow-internal2"
                    :kind               "SurveyedLocale"
                    :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflow-internal2/akvoflow-internal2.p12"}
                   "gae-internal2.strs")

  (gae->local-file {:service-account-id "sa-akvoflowsandbox@akvoflowsandbox.iam.gserviceaccount.com"
                    :org-id             "akvoflowsandbox"
                    :kind               "SurveyedLocale"
                    :private-key-file   "/Users/dlebrero/projects/akvo/akvo-flow-server-config/akvoflowsandbox/akvoflowsandbox.p12"}
                   "gae.strs")

  (local-file->kafka-binary "gae.strs" "binarytest")

  (local-file->kafka-avro "gae.strs" "nonbinarytest" (org.akvo.flow.avro.DataPoint/getClassSchema))

  (local-file/read-from "gae-internal2.strs"
                        (comp
                          (fn [stats] (gae-to-kafka-initial-import.avro-schema/->avro stats "Foo"))
                          gae-to-kafka-initial-import.avro-schema/collect-stats
                          (partial map (comp
                                         gae-entity->clj
                                         bytes->obj
                                         str->bytes))))


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
