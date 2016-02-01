(ns datastore-test
  (:import [com.google.gcloud.datastore.testing LocalGcdHelper]
           [com.google.gcloud.datastore NullValue Entity])
  (:require [gclouj.datastore :refer :all]
            [clojure.test :refer :all]))

(def project-id "gclouj-datastore")

(deftest entity-mapping
  (let [testbytes (byte-array [1 2 3 4 5])
        e (entity (incomplete-key project-id "Foo")
                  {"FirstName"    "Paul"
                   "Age"          35
                   "Clojurist"    true
                   "NilValue"     nil
                   "TestBytes"    testbytes
                   "Intelligence" 50.5
                   "Address"      {"City" "London"}})]
    (is (= "Paul"            (.getString e "FirstName")))
    (is (= 35                (.getLong e "Age")))
    (is (= 50.5              (.getDouble e "Intelligence")))
    (is (true?               (.getBoolean e "Clojurist")))
    (is (instance? NullValue (.getValue e "NilValue")))
    (is (= (seq testbytes)   (seq (.toByteArray (.getBlob e "TestBytes")))))
    (is (= "London"          (-> e (.getEntity "Address") (.getString "City"))))))

  (deftest put-and-retrieve-entity
    (let [port (LocalGcdHelper/findAvailablePort 9900)
          helper (LocalGcdHelper/start project-id port)]
      (let [opts (test-options project-id port)
            s    (service opts)
            t    (transaction s)]
        (let [added         (add-entity t (entity (incomplete-key project-id "Foo")
                                                  {"FirstName" "Paul"}))
              added-key     (.key added)
              added2        (add-entity t (entity (incomplete-key project-id "Foo")
                                                  {"FirstName" "Peter"}))
              added-key2    (.key added2)
              added3        (add-entity t (entity (incomplete-key project-id "Foo")
                                                  {"FirstName" "Arthur"}))
              added-key3    (.key added3)]
          (.commit t)
          (let [t       (transaction s)
                fetched (get-entity t added-key)]
            (is (= "Paul" (.getString fetched "FirstName")))
            (let [new-entity (Entity/builder fetched)]
              (.set new-entity "FirstName" "Arthur"))
            (is (= 3 (count (get-entity t added-key added-key2 added-key3))))
            (is (= "Arthur" (-> (get-entity t added-key added-key2 added-key3)
                                (last)
                                (.getString "FirstName")))))))
      (.stop helper)))

(deftest querying
  (let [port   (LocalGcdHelper/findAvailablePort 9900)
        helper (LocalGcdHelper/start project-id port)
        s      (-> (test-options project-id port)
                   (service))]
    ;; create some entities to query for
    (let [t (transaction s)]
      (add-entity t (entity (incomplete-key project-id "QueryFoo") {"Name" "Paul"
                                                                    "Age"  35}))
      (add-entity t (entity (incomplete-key project-id "QueryFoo") {"Name" "Pat"
                                                                    "Age"  30}))
      (.commit t))

    (let [nothing (query-entities s {:kind "BadKind"})
          found   (query-entities s {:kind    "QueryFoo"
                                     :filters [(efilter := "Name" "Paul")]})]
      (is (= 0 (count nothing)))
      (is (= 1 (count found)))
      (is (= "Paul" (.getString (first found) "Name"))))

    (.stop helper)))
