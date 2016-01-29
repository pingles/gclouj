(ns gclouj.datastore
  (:require [clojure.java.io :as io])
  (:import [gclouj DatastoreOptionsFactory]
           [com.google.gcloud.datastore DatastoreOptions Entity FullEntity DatastoreOptions$DefaultDatastoreFactory Transaction TransactionOption Key IncompleteKey DatastoreBatchWriter]
           [com.google.gcloud AuthCredentials]))

(defn credential-options [project-id namespace json-key]
  (DatastoreOptionsFactory/create project-id namespace (AuthCredentials/createForJson (io/input-stream json-key))))

(defn test-options [project-id port]
  (DatastoreOptionsFactory/createTestOptions project-id port))

(defn service [options]
  (.service options))

(defmulti complete-key (fn [project-id kind name-or-id] (cond (string? name-or-id) :name
                                                             (number? name-or-id) :id)))
(defmethod complete-key :name [project-id kind name] (.build (Key/builder project-id kind ^String name)))
(defmethod complete-key :id [project-id kind id] (.build (Key/builder project-id kind ^long id)))

(defn incomplete-key [project-id kind]
  (.build (IncompleteKey/builder project-id kind)))


(defn get-entity [^Transaction txn key]
  (.get txn key))

(defn add-entity [^Transaction txn entity]
  (.add txn ^FullEntity entity))

(defn entity [key m]
  (let [b (Entity/builder key)]
    (doseq [[k v] m]
      (cond (associative? v) (let [attr-key (incomplete-key (.projectId key)
                                                            (format "%s.%s" (.kind key) k))]
                               (.set b k (entity attr-key v)))
            :else            (.set b k v)))
    (.build b)))

(defn transaction [service]
  (.newTransaction service (into-array TransactionOption [])))
