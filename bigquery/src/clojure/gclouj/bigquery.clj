(ns gclouj.bigquery
  (:require [clojure.walk :as walk]
            [clj-time.coerce :as tc])
  (:import [com.google.cloud.bigquery BigQueryOptions BigQuery$DatasetListOption DatasetInfo DatasetId BigQuery$TableListOption StandardTableDefinition TableId BigQuery$DatasetOption BigQuery$TableOption Schema Field Field$Type Field$Mode StandardTableDefinition$StreamingBuffer InsertAllRequest InsertAllRequest$RowToInsert InsertAllResponse BigQueryError BigQuery$DatasetDeleteOption QueryRequest QueryResponse QueryResult JobId Field Field$Type$Value FieldValue FieldValue$Attribute LoadConfiguration BigQuery$JobOption JobInfo$CreateDisposition JobInfo$WriteDisposition JobStatistics JobStatistics$LoadStatistics JobStatus JobStatus$State FormatOptions UserDefinedFunction JobInfo ExtractJobConfiguration LoadJobConfiguration QueryJobConfiguration QueryJobConfiguration$Priority Table BigQuery$QueryResultsOption TableInfo ViewDefinition CsvOptions CopyJobConfiguration]
           [gclouj BigQueryOptionsFactory]
           [com.google.common.hash Hashing]
           [java.util List Collections]
           [java.util.concurrent TimeUnit]))


(defmulti field-value->clojure (fn [attribute val]
                                 attribute))
(defmethod field-value->clojure FieldValue$Attribute/PRIMITIVE [_ ^FieldValue val]
  (.value val))

(defprotocol ToClojure
  (to-clojure [_]))
(extend-protocol ToClojure
  DatasetId
  (to-clojure [x] {:dataset-id (.dataset x)
                   :project-id (.project x)})
  DatasetInfo
  (to-clojure [x] {:creation-time (.creationTime x)
                   :dataset-id    (to-clojure (.datasetId x))
                   :description   (.description x)
                   :friendly-name (.friendlyName x)
                   :location      (.location x)
                   :last-modified (.lastModified x)})
  TableId
  (to-clojure [x] {:dataset-id (.dataset x)
                   :project-id (.project x)
                   :table-id   (.table x)})
  StandardTableDefinition$StreamingBuffer
  (to-clojure [x] {:estimated-bytes   (.estimatedBytes x)
                   :estimated-rows    (.estimatedRows x)
                   :oldest-entry-time (.oldestEntryTime x)})
  StandardTableDefinition
  (to-clojure [x] {:location         (.location x)
                   :bytes            (.numBytes x)
                   :rows             (.numRows x)
                   :streaming-buffer (when-let [sb (.streamingBuffer x)] (to-clojure sb))
                   :schema           (when-let [schema (.schema x)] (to-clojure schema))})
  ViewDefinition
  (to-clojure [x] {:schema (when-let [schema (.schema x)]
                             (to-clojure schema))})
  Table
  (to-clojure [x] {:creation-time (.creationTime x)
                   :description   (.description x)
                   :friendly-name (.friendlyName x)
                   :table-id      (to-clojure (.tableId x))
                   :definition    (to-clojure (.definition x))})
  BigQueryError
  (to-clojure [x] {:reason   (.reason x)
                   :location (.location x)
                   :message  (.message x)})
  InsertAllResponse
  (to-clojure [x] {:errors (->> (.insertErrors x)
                                (map (fn [[idx errors]]
                                       {:index  idx
                                        :errors (map to-clojure errors)}))
                                (seq))})
  JobId
  (to-clojure [x] {:project-id (.project x)
                   :job-id     (.job x)})
  Field
  (to-clojure [x] (let [type (.. x type value)]
                    {:name   (.name x)
                     :mode   ({Field$Mode/NULLABLE :nullable
                               Field$Mode/REPEATED :repeated
                               Field$Mode/REQUIRED :required} (.mode x))
                     :type   ({Field$Type$Value/BOOLEAN   :bool
                               Field$Type$Value/FLOAT     :float
                               Field$Type$Value/INTEGER   :integer
                               Field$Type$Value/RECORD    :record
                               Field$Type$Value/STRING    :string
                               Field$Type$Value/TIMESTAMP :timestamp} type)
                     :fields (when (= Field$Type$Value/RECORD type)
                               (map to-clojure (.fields x)))}))
  FieldValue
  (to-clojure [x] (field-value->clojure (.attribute x) x))
  Schema
  (to-clojure [x] (map to-clojure (.fields x)))
  QueryResponse
  (to-clojure [x] (let [completed (.jobCompleted x)]
                    {:completed? completed
                     :errors     (->> (.executionErrors x) (map to-clojure) (seq))
                     :job-id     (to-clojure (.jobId x))
                     :results    (when completed
                                   (map (fn [fields] (map to-clojure fields))
                                        (iterator-seq (.. x result iterateAll))))
                     :schema     (when completed
                                   (to-clojure (.. x result schema)))
                     :cache-hit  (when completed
                                   (.. x result cacheHit))}))
  JobStatistics$LoadStatistics
  (to-clojure [x] {:input-bytes  (.inputBytes x)
                   :input-files  (.inputFiles x)
                   :output-bytes (.outputBytes x)
                   :output-rows  (.outputRows x)})
  JobStatistics
  (to-clojure [x] {:created (.creationTime x)
                   :end     (.endTime x)
                   :started (.startTime x)})
  JobStatus
  (to-clojure [x] {:state  ({JobStatus$State/DONE    :done
                             JobStatus$State/PENDING :pending
                             JobStatus$State/RUNNING :running} (.state x))
                   :errors (seq (map to-clojure (.executionErrors x)))})
  JobInfo
  (to-clojure [x] {:job-id     (to-clojure (.jobId x))
                   :statistics (to-clojure (.statistics x))
                   :email      (.userEmail x)
                   :status     (to-clojure (.status x))}))

(defn service
  ([] (.service (BigQueryOptions/defaultInstance)))
  ([{:keys [project-id] :as options}]
   (.service (BigQueryOptionsFactory/create project-id))))

(defn datasets [service]
  (let [it (-> service
               (.listDatasets (into-array BigQuery$DatasetListOption []))
               (.iterateAll))]
    (map to-clojure (iterator-seq it))))

(defn dataset [service {:keys [project-id dataset-id] :as dataset}]
  (when-let [dataset (.getDataset service (DatasetId/of project-id dataset-id) (into-array BigQuery$DatasetOption []))]
    (to-clojure dataset)))

(defn tables
  "Returns a sequence of table-ids. For complete table
  information (schema, location, size etc.) you'll need to also use the
  `table` function."
  [service {:keys [project-id dataset-id] :as dataset}]
  (let [it (-> service
               (.listTables (DatasetId/of project-id dataset-id)
                            (into-array BigQuery$TableListOption []))
               (.iterateAll))]
    (map to-clojure (iterator-seq it))))

(defn table [service {:keys [project-id dataset-id table-id] :as table}]
  (to-clojure (.getTable service
                         (TableId/of project-id dataset-id table-id)
                         (into-array BigQuery$TableOption []))))

(defn create-dataset [service {:keys [project-id dataset-id friendly-name location description table-lifetime-millis] :as dataset}]
  (let [locations {:eu "EU"
                   :us "US"}
        builder   (DatasetInfo/builder project-id dataset-id)]
    (when friendly-name
      (.friendlyName builder friendly-name))
    (when description
      (.description builder description))
    (when table-lifetime-millis
      (.defaultTableLifetime builder table-lifetime-millis))
    (.location builder (or (locations location) "US"))
    (to-clojure (.create service (.build builder) (into-array BigQuery$DatasetOption [])))))

(defn delete-dataset [service {:keys [project-id dataset-id delete-contents?] :as dataset}]
  (let [options (if delete-contents?
                  [(BigQuery$DatasetDeleteOption/deleteContents)]
                  [])]
    (.delete service
             (DatasetId/of project-id dataset-id)
             (into-array BigQuery$DatasetDeleteOption options))))

(defn- mkfield [{:keys [name type description mode fields]}]
  (let [field-type (condp = type
                     :bool (Field$Type/bool)
                     :float (Field$Type/floatingPoint)
                     :integer (Field$Type/integer)
                     :string (Field$Type/string)
                     :timestamp (Field$Type/timestamp)
                     :record (Field$Type/record ^List (map mkfield fields)))
        builder    (Field/builder name field-type)
        field-mode ({:nullable (Field$Mode/NULLABLE)
                     :repeated (Field$Mode/REPEATED)
                     :required (Field$Mode/REQUIRED)} (or mode :nullable))]
    (.mode builder field-mode)
    (.build builder)))

(defn- mkschema
  [fields]
  (let [builder (Schema/builder)]
    (.fields builder (into-array Field (->> fields (map mkfield))))
    (.build builder)))

(defn create-table
  "Fields: sequence of fields representing the table schema.
  e.g. [{:name \"foo\" :type :record :fields [{:name \"bar\" :type :integer}]}]"
  [service {:keys [project-id dataset-id table-id] :as table} fields]
  (let [builder    (TableInfo/builder (TableId/of project-id dataset-id table-id)
                                      (StandardTableDefinition/of (mkschema fields)))
        table-info (.build builder)]
    (to-clojure (.create service table-info (into-array BigQuery$TableOption [])))))

(defn delete-table
  [service {:keys [project-id dataset-id table-id] :as table}]
  (.delete service (TableId/of project-id dataset-id table-id)))

(defn row-hash
  "Creates a hash suitable for identifying duplicate rows, useful when
  streaming to avoid inserting duplicate rows."
  [m & {:keys [bits] :or {bits 128}}]
  (-> (Hashing/goodFastHash bits) (.hashUnencodedChars (pr-str m)) (.toString)))

(defn- row-value [m]
  ;; the google client incorrectly interprets clojure maps as arrays so
  ;; we wrap in an unmodifiableMap to ensure the client interprets
  ;; correctly.
  (letfn [(wrap-map [x]
            (if (map? x)
              (Collections/unmodifiableMap x)
              x))]
    (walk/postwalk wrap-map m)))

(defn- insert-row [row-id row]
  (if row-id
    (InsertAllRequest$RowToInsert/of (row-id row) (row-value row))
    (InsertAllRequest$RowToInsert/of (row-value row))))

(defn insert-all
  "Performs a streaming insert of rows. row-id can be a function to
  return the unique identity of the row (e.g. row-hash). Template suffix
  can be used to create tables according to a template."
  [service {:keys [project-id dataset-id table-id skip-invalid? template-suffix row-id] :as table} rows]
  (let [builder (InsertAllRequest/builder (TableId/of project-id dataset-id table-id)
                                          ^Iterable (map (partial insert-row row-id) rows))]
    (when template-suffix
      (.templateSuffix builder template-suffix))
    (->> builder
         (.build)
         (.insertAll service)
         (to-clojure))))



(defn query
  "Executes a query. BigQuery will create a Query Job and block for the
  specified timeout. If the query returns within the time the results
  will be returned. Otherwise, results need to be retrieved separately
  using query-results. Status of the job can be checked using the job
  function, and checking completed?"
  [service query {:keys [max-results dry-run? max-wait-millis use-cache? use-legacy-sql? default-dataset] :as query-opts}]
  (let [builder (QueryRequest/builder query)]
    (when default-dataset
      (let [{:keys [project-id dataset-id]} default-dataset]
        (.defaultDataset builder (DatasetId/of project-id dataset-id))))
    (when max-results
      (.maxResults builder max-results))
    (when-not (nil? dry-run?)
      (.dryRun builder dry-run?))
    (when max-wait-millis
      (.maxWaitTime builder max-wait-millis))
    (when-not (nil? use-legacy-sql?)
      (.useLegacySql builder use-legacy-sql?))
    (.useQueryCache builder use-cache?)
    (let [q (.build builder)]
      (to-clojure (.query service q)))))




(defmulti query-option (fn [[type _]] type))
(defmethod query-option :max-wait-millis [[_ val]] (BigQuery$QueryResultsOption/maxWaitTime val))

(defn query-results
  "Retrieves results for a Query job. Will throw exceptions unless Job
  has completed successfully. Check using job and completed? functions."
  [service {:keys [project-id job-id] :as job} & {:keys [max-wait-millis] :as opts}]
  {:pre [(string? job-id) (string? project-id)]}
  (to-clojure (.getQueryResults service
                                (JobId/of project-id job-id)
                                (->> opts (map query-option) (into-array BigQuery$QueryResultsOption)))))

(defn- parse-timestamp [val]
  (let [seconds (long (Double/valueOf val))
        millis  (.toMillis (TimeUnit/SECONDS) seconds)]
    (tc/from-long millis)))

(def cell-coercions {:integer   #(Long/valueOf %)
                     :bool      #(Boolean/valueOf %)
                     :float     #(Double/valueOf %)
                     :string    identity
                     :timestamp parse-timestamp})

(defn- coerce-result
  [schema]
  (let [coercions (map cell-coercions (map :type schema))
        names     (map :name schema)]
    (fn [row]
      (->> (for [[name coerce value] (partition 3 (interleave names coercions row))]
             [name (coerce value)])
           (into {})))))

(defn query-results-seq
  "Takes a query result and coerces the results from being raw sequences
  into maps according to the schema and coercing values according to
  their type. e.g.:
  converts from query results of: {:results ((\"500\")) :schema ({:name \"col\" :type :integer})} into...
  ({\"col\" 500} ...)"
  [{:keys [results schema] :as query-results}]
  (map (coerce-result schema) results))

(defn job [service {:keys [project-id job-id] :as job}]
  (to-clojure (.getJob service (JobId/of project-id job-id) (into-array BigQuery$JobOption []))))

(defn successful? [job]
  (and (= :done (get-in job [:status :state]))
       (empty? (get-in job [:status :errors]))))

(defn running? [job]
  (= :running (get-in job [:status :state])))

(def create-dispositions {:needed JobInfo$CreateDisposition/CREATE_IF_NEEDED
                          :never  JobInfo$CreateDisposition/CREATE_NEVER})

(def write-dispositions {:append   JobInfo$WriteDisposition/WRITE_APPEND
                         :empty    JobInfo$WriteDisposition/WRITE_EMPTY
                         :truncate JobInfo$WriteDisposition/WRITE_TRUNCATE})

(defn table-id [{:keys [project-id dataset-id table-id]}]
  (TableId/of project-id dataset-id table-id))

(defn execute-job [service job]
  (to-clojure (.create service (.build (JobInfo/builder job)) (into-array BigQuery$JobOption []))))

(defn load-job
  "Loads data from Cloud Storage URIs into the specified table.
  Table argument needs to be a map with project-id, dataset-id and table-id.
  Options:
  `create-disposition` controls whether tables are created if
  necessary, or assume to have been created already (default).
  `write-disposition`  controls whether data should :append (default),
  :truncate or :empty to fail if table exists.
  :format              :json or :csv
  :schema              sequence describing the table schema.[{:name \"foo\" :type :record :fields [{:name \"bar\" :type :integer}]}]"
  [service table {:keys [format create-disposition write-disposition max-bad-records schema]} uris]
  (let [builder (LoadJobConfiguration/builder (table-id table)
                                              uris
                                              ({:json (FormatOptions/json)
                                                :csv  (FormatOptions/csv)} (or format :json)))]
    (.createDisposition builder (create-dispositions (or create-disposition :never)))
    (.writeDisposition builder (write-dispositions (or write-disposition :append)))
    (.maxBadRecords builder (int (or max-bad-records 0)))
    (when schema
      (.schema builder (mkschema schema)))
    (execute-job service (.build builder))))

(def extract-format {:json "NEWLINE_DELIMITED_JSON"
                     :csv  "CSV"
                     :avro "AVRO"})

(def extract-compression {:gzip "GZIP"
                          :none "NONE"})

(defn extract-job
  "Extracts data from BigQuery into a Google Cloud Storage location.
   Table argument needs to be a map with project-id, dataset-id and table-id."
  [service table destination-uri & {:keys [format compression]
                                    :or   {format      :json
                                           compression :gzip}}]
  (let [builder (ExtractJobConfiguration/builder (table-id table) destination-uri)]
    (.format builder (extract-format format))
    (.compression builder (extract-compression compression))
    (execute-job service (.build builder))))

(defn copy-job
  [service sources destination & {:keys [create-disposition write-disposition]
                                  :or   {create-disposition :needed
                                         write-disposition  :empty}}]
  (let [builder (CopyJobConfiguration/builder (table-id destination) (map table-id sources))]
    (.createDisposition builder (create-dispositions create-disposition))
    (.writeDisposition builder (write-dispositions write-disposition))
    (execute-job service (.build builder))))

(defn user-defined-function
  "Creates a User Defined Function suitable for use in BigQuery queries. Can be a Google Cloud Storage uri (e.g. gs://bucket/path), or an inline JavaScript code blob."
  [udf]
  (if (.startsWith udf "gs://")
    (UserDefinedFunction/fromUri udf)
    (UserDefinedFunction/inline udf)))

(defn query-job
  [service query {:keys [create-disposition write-disposition large-results? dry-run? destination-table default-dataset use-cache? flatten-results? use-legacy-sql? priority udfs]}]
  (let [priorities {:batch       (QueryJobConfiguration$Priority/BATCH)
                    :interactive (QueryJobConfiguration$Priority/INTERACTIVE)}
        builder    (QueryJobConfiguration/builder query)]
    (when default-dataset
      (let [{:keys [project-id dataset-id]} default-dataset]
        (.defaultDataset builder (DatasetId/of project-id dataset-id))))
    (.createDisposition builder (create-dispositions (or create-disposition :never)))
    (.writeDisposition builder (write-dispositions (or write-disposition :append)))
    (.useLegacySql builder use-legacy-sql?)
    (.allowLargeResults builder large-results?)
    (.useQueryCache builder use-cache?)
    (.flattenResults builder flatten-results?)
    (.priority builder (priorities (or priority :batch)))
    (when udfs
      (.userDefinedFunctions builder udfs))
    (when destination-table
      (let [{:keys [project-id dataset-id table-id]} destination-table]
        (.destinationTable builder (TableId/of project-id dataset-id table-id))))
    (when-not (nil? dry-run?)
      (.dryRun builder dry-run?))
    (execute-job service (.build builder))))
