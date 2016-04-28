(ns gclouj.datastore
  (:require [clojure.java.io :as io]
            [clojure.walk :as w])
  (:import [gclouj DatastoreOptionsFactory]
           [java.io InputStream]
           [java.nio ByteBuffer]
           [com.google.gcloud.datastore DatastoreOptions Entity FullEntity DatastoreOptions$DefaultDatastoreFactory Transaction Key IncompleteKey DatastoreBatchWriter EntityValue ValueType StringValue LongValue DoubleValue DateTime DateTimeValue BooleanValue BlobValue Blob NullValue Value KeyValue FullEntity$Builder Query StructuredQuery$PropertyFilter StructuredQuery$CompositeFilter StructuredQuery$Filter StructuredQuery$OrderBy]
           [com.google.gcloud AuthCredentials]))

(defn credential-options [project-id namespace json-key]
  (DatastoreOptionsFactory/create project-id namespace (AuthCredentials/createForJson (io/input-stream json-key))))

(defn test-options [project-id port]
  (DatastoreOptionsFactory/createTestOptions project-id port))

(defn service
  "Creates the Datastore service to be used with other functions. Use
  options when connecting outside of the Google Cloud Platform, if
  you're running on GCE/GAE you can use the no-args version."
  ([] (.service (DatastoreOptions/defaultInstance)))
  ([options] (.service options)))

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

(defn delete-entity [^Transaction txn & keys]
  (.delete txn (into-array Key keys)))

(defn update-entity [^Transaction txn & entities]
  (.update txn (into-array Entity entities)))

(defn byte-array? [x]
  (= (Class/forName "[B") (class x)))

(defn property-value [x]
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
  all values into an entity or value using property-value. "
  [key m & {:keys [index]}]
  (let [builder    (Entity/builder key)]
    (doseq [[k v] m]
      (try
        (cond (map? v) (let [attrkey (incomplete-key (.projectId key) (format "%s.%s" (.kind key) k))]
                       (.set builder k (entity attrkey v)))
              :else    (.set builder k (property-value v)))
        (catch IllegalArgumentException e
          (throw (ex-info (str "error mapping " k " with value " v) {})))))
    (.build builder)))

(defprotocol ToClojure
  (to-clojure [x]))
(extend-protocol ToClojure
  BlobValue
  (to-clojure [value] (.. value asReadOnlyByteBuffer))
  BooleanValue
  (to-clojure [value] (.get value))
  DateTimeValue
  (to-clojure [value] (.get value))
  DoubleValue
  (to-clojure [value] (.get value))
  KeyValue
  (to-clojure [value] (.get value))
  NullValue
  (to-clojure [value] nil)
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

(defn transaction [service]
  (.newTransaction service))

(defmulti efilter (fn [type & filter-expression] type))
(defmethod efilter :ancestor [_ key]
  (StructuredQuery$PropertyFilter/hasAncestor key))
(defmethod efilter := [_ property value]
  (StructuredQuery$PropertyFilter/eq property value))
(defmethod efilter :>= [_ property value]
  (StructuredQuery$PropertyFilter/ge property value))
(defmethod efilter :> [_ property value]
  (StructuredQuery$PropertyFilter/gt property value))
(defmethod efilter :nil [_ property]
  (StructuredQuery$PropertyFilter/isNull property))
(defmethod efilter :<= [_ property value]
  (StructuredQuery$PropertyFilter/le property value))
(defmethod efilter :< [_ property value]
  (StructuredQuery$PropertyFilter/lt property value))

(defn query-filters
  ([f1] f1)
  ([f1 f2 & more]
   (StructuredQuery$CompositeFilter/and f1 (into-array StructuredQuery$Filter (conj more f2)))))

(defn order-by [direction property]
  (condp = direction
    :desc (StructuredQuery$OrderBy/desc property)
    :asc  (StructuredQuery$OrderBy/asc property)))

(defn query
  "Query can specify: kind (string) and a sequence of filters. Runner
  can be a service or transaction. Only ancestor filters are able to be
  run by transactions.
  (query service {:kind \"Foo\"
                  :order   [[:desc \"Age\"]]
                  :filters ['(:= \"Name\" \"Paul\")]}"
  [runner {:keys [kind filters order limit offset]} & {:keys [query-type]
                                                       :or   {query-type :entity}}]
  {:pre [(contains? #{:entity :key} query-type)]}
  (let [builder (condp = query-type
                  :entity (Query/entityQueryBuilder)
                  :key    (Query/keyQueryBuilder))]
    (when kind
      (.kind builder kind))
    (when (seq filters)
      (let [fs (->> filters
                    (map (fn [[expr & args]]
                           (apply efilter expr args))))
            query-filter (apply query-filters fs)]
        (.filter builder query-filter)))
    (when-let [os (seq (->> order (map (fn [[direction property]]
                                         (order-by direction property)))))]
      (cond (= (count os) 1) (.orderBy builder (first os) (into-array StructuredQuery$OrderBy []))
            :else            (.orderBy builder (first os) (into-array StructuredQuery$OrderBy (rest os)))))
    (when limit
      (.limit builder (int limit)))
    (when offset
      (.offset builder (int offset)))
    (let [query (.build builder)]
      (iterator-seq (.run runner query)))))
