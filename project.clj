(defproject gae-to-kafka-initial-import "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]

                 [org.clojure/tools.logging "0.4.0"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]

                 [org.akvo/commons "0.4.2" :exclusions [[org.clojure/tools.reader]]]
                 [spec-provider "0.4.11"]
                 [cheshire "5.8.0"]

                 [org.akvo/kfk.avro-bridge "1.4.4d8c1b74e8d85e9ea03292c7952ce093b1b88e71"]
                 [org.akvo.flow/flow-avro-schemas "1.0.10.e1101a2b6ac574ff63b4824118cadf843b03064c"]

                 [com.taoensso/nippy "2.13.0"]
                 [org.apache.commons/commons-lang3 "3.7"]
                 [http.async.client "1.2.0"]

                 ;; GAE SDK
                 [com.google.appengine/appengine-tools-sdk "1.9.53"]
                 [com.google.appengine/appengine-remote-api "1.9.53"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.9.53"]


                 ])
