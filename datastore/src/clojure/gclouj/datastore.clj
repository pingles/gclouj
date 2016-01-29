(ns gclouj.datastore
  (:require [clojure.java.io :as io])
  (:import [gclouj DatastoreOptionsFactory]
           [com.google.gcloud.datastore DatastoreOptions Entity DatastoreOptions$DefaultDatastoreFactory Transaction TransactionOption Key]
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

(defn get-entity [^Transaction txn key]
  (.get txn key))

(defn transaction [service]
  (.newTransaction service (into-array TransactionOption [])))
