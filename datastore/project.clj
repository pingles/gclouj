(defproject gclouj/datastore "0.1.1-SNAPSHOT"
  :description "Google Cloud Datastore"
  :url "https://github.com/pingles/gclouj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.google.gcloud/gcloud-java-datastore "0.1.7" :exclusions [com.google.guava/guava com.google.guava/guava-jdk5]]
                 [com.google.guava/guava "19.0"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}}})
