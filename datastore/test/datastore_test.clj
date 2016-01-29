(ns datastore-test
  (:import [com.google.gcloud.datastore.testing LocalGcdHelper])
  (:require [gclouj.datastore :refer :all]
            [clojure.test :refer :all]))

(def project-id "gclouj-datastore")

(deftest entity-mapping
  (let [e (entity (complete-key project-id "Foo" "name") {"FirstName" "Paul"})]
    (is (= "Paul" (.getString e "FirstName")))))

(deftest put-and-retrieve-entity
  (let [port (LocalGcdHelper/findAvailablePort 9900)
        helper (LocalGcdHelper/start project-id port)]
    (let [opts (test-options project-id port)
          s    (service opts)
          t    (transaction s)]
      (let [added         (add-entity t (entity (incomplete-key project-id "Foo")
                                                {"FirstName" "Paul"}))
            completed-key (.key added)]
        (.commit t)
        (let [t       (transaction s)
              fetched (get-entity t completed-key)]
          (is (= "Paul" (.getString fetched "FirstName")))
          (.rollback t))))

    (.stop helper)))
