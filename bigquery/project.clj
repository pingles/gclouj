(defproject gclouj/bigquery "0.2.5.2"
  :description "Google Cloud BigQuery"
  :url "https://github.com/pingles/gclouj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.google.cloud/gcloud-java-bigquery "0.2.5"]
                 [clj-time "0.11.0"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}}})
