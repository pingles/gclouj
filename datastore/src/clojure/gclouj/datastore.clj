(ns gclouj.datastore
  (:require [clojure.java.io :as io]
            [clojure.walk :as w])
  (:import [gclouj DatastoreOptionsFactory]
           [java.io InputStream]
           [java.nio ByteBuffer]
           [com.google.gcloud.datastore DatastoreOptions Entity FullEntity DatastoreOptions$DefaultDatastoreFactory Transaction TransactionOption Key IncompleteKey DatastoreBatchWriter EntityValue ValueType StringValue LongValue DoubleValue DateTime DateTimeValue BooleanValue BlobValue Blob NullValue Value KeyValue FullEntity$Builder]
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

(defn get-entity
  ([^Transaction txn key]
   (.get txn key))
  ([^Transaction txn key1 key2]
   (iterator-seq (.get txn (into-array Key [key1 key2]))))
  ([^Transaction txn key1 key2 & keys]
   (iterator-seq (.get txn (into-array Key (concat [key1 key2] keys))))))

(defn add-entity [^Transaction txn entity]
  (.add txn ^FullEntity entity))

(defn byte-array? [x]
  (= (Class/forName "[B") (class x)))

(defn attrvalue [x]
  (cond (nil? x)                      (NullValue. )
        (float? x)                    (DoubleValue/of x)
        (number? x)                   (LongValue/of x)
        (string? x)                   (StringValue/of x)
        (instance? DateTime x)        (DateTimeValue/of x)
        (instance? Boolean x)         (BooleanValue/of x)
        (instance? Key x)             (KeyValue/of x)
        (or (byte-array? x)
            (instance? InputStream x)
            (instance? ByteBuffer x)) (BlobValue/of (Blob/copyFrom x))
        :else      x))

(defn entity
  "Converts a Clojure map into a Datastore Entity. Recursively converts
  all values into an entity or value using attrvalue."
  [key m]
  (let [builder    (Entity/builder key)]
    (doseq [[k v] m]
      (cond (map? v) (let [attrkey (incomplete-key (.projectId key) (format "%s.%s" (.kind key) k))]
                       (.set builder k (entity attrkey v)))
            :else    (.set builder k (attrvalue v))))
    (.build builder)))


(defprotocol ToClojure
  (to-clojure [x]))
(extend-protocol ToClojure
  LongValue
  (to-clojure [value] (.get value))
  StringValue
  (to-clojure [value] (.get value))
  EntityValue
  (to-clojure [value] (to-clojure (.get value)))
  FullEntity
  (to-clojure [entity]
    (let [names (-> entity (.names) (.iterator) (iterator-seq))]
      (into {} (map (fn [attr] [attr (to-clojure (.getValue entity attr))]) names)))))

(defn transaction [service & options]
  (.newTransaction service (into-array TransactionOption [])))
