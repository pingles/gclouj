(defproject gclouj/bigquery "0.2.7.0"
  :description "Google Cloud BigQuery"
  :url "https://github.com/pingles/gclouj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.google.cloud/gcloud-java-bigquery "0.2.8" :exclusions [io.netty/netty-codec-http2 io.grpc/grpc-core]]
                 [clj-time "0.14.0"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}}})
