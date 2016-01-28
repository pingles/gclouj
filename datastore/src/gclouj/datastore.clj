(ns gclouj.datastore
  (:import [com.google.gcloud.datastore DatastoreOptions Transaction TransactionOption Key]))

(defn service [namespace]
  (-> (DatastoreOptions/builder)
      (.namespace namespace)
      (.build)
      (.service)))

(defn complete-key [project-id kind name-or-id]
  (.build (Key/builder project-id kind name-or-id)))

(defn get-entity [txn & keys]
  (.get txn (into-array Key keys)))

(defn transaction [service]
  (.newTransaction service (into-array TransactionOption [])))
