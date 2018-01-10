(ns gae-to-kafka-initial-import.io.kafka-rest-proxy
  (:require [cheshire.core :as json]
            [http.async.client :as http]
            [http.async.client.request :as http-req])
  (:import (com.ning.http.client Request)
           (java.util Base64)))

(defn send-request [client {:keys [method url] :as req}]
  (let [request ^Request (apply http-req/prepare-request method url (apply concat (dissoc req :method :url)))
        response (http/await (http-req/execute-request client request))]
    (assoc response
      :status (http/status response)
      :body (http/string response)
      :error (http/error response)
      :headers (http/headers response))))

(defn create-client [{:keys [connection-timeout request-timeout max-connections]}]
  {:pre [connection-timeout request-timeout max-connections]}
  (http/create-client
    :connection-timeout connection-timeout
    :request-timeout request-timeout
    :read-timeout request-timeout
    :max-conns-per-host max-connections
    :max-conns-total max-connections
    :idle-in-pool-timeout 60000))

(defn push-as-binary [topic byte-arrays-as-base64-strs]
  (with-open [http-client (create-client {:connection-timeout 10000
                                          :request-timeout    10000
                                          :max-connections    10})]
    (doseq [batch (partition 100 byte-arrays-as-base64-strs)]
      (send-request http-client
                    {:method  :post
                     :headers {"content-type" "application/vnd.kafka.binary.v2+json"
                               "Accept"       "application/vnd.kafka.v2+json"}
                     :url     (str "http://kafka-rest-proxy.akvotest.org/topics/" topic)
                     :body    (json/generate-string {:records
                                                     (map (fn [o]
                                                            {:value o})
                                                          batch)})}))))
