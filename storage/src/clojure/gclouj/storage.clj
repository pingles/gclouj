(ns gclouj.storage
  (:import [com.google.cloud AuthCredentials]
           [com.google.cloud.storage StorageOptions Storage$BucketListOption Bucket]
           [gclouj StorageOptionsFactory])
  (:require [clojure.java.io :as io]
            [clj-time.coerce :as tc]))


(defprotocol ToClojure
  (to-clojure [x]))

(extend-protocol ToClojure
  Bucket
  (to-clojure [bucket] {:name          (.name bucket)
                        :created       (tc/from-long (.createTime bucket))
                        :storage-class (.storageClass bucket)
                        :location      (.location bucket)}))

(defn credential-options [project-id json-key]
  (StorageOptionsFactory/create project-id (AuthCredentials/createForJson (io/input-stream json-key))))

(defn service
  ([] (.service (StorageOptions/defaultInstance)))
  ([^StorageOptions options] (.service options)))

; storage.list().iterateAll()
(defn buckets [service]
  (->> (.iterateAll (.list service (into-array Storage$BucketListOption [])))
       (iterator-seq)
       (map to-clojure)))
