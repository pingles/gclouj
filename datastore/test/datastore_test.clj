(ns datastore-test
  (:import [com.google.gcloud.datastore.testing LocalGcdHelper])
  (:require [gclouj.datastore :as ds]
            [clojure.test :refer :all]))

(def project-id "gclouj-datastore")

(deftest retrieve-entity
  (let [port (LocalGcdHelper/findAvailablePort 9900)
        helper (LocalGcdHelper/start project-id port)]
    (let [opts (ds/test-options project-id port)
          s    (ds/service opts)
          t    (ds/transaction s)]
      (is (nil? (ds/get-entity t (ds/complete-key project-id "Foo" "name")))))
    (.stop helper)))
