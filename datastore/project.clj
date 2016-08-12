(defproject gclouj/datastore "0.2.1"
  :description "Google Cloud Datastore"
  :url "https://github.com/pingles/gclouj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.google.cloud/gcloud-java-datastore "0.2.6" :exclusions [com.google.guava/guava com.google.guava/guava-jdk5]]
                 [com.google.guava/guava "19.0"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}}})
