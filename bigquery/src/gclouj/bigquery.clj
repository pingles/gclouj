(ns gclouj.bigquery
  (:import [com.google.gcloud.bigquery BigQueryOptions BigQuery$DatasetListOption DatasetInfo DatasetId BigQuery$TableListOption TableInfo TableId BigQuery$DatasetOption BigQuery$TableOption Schema Field Field$Type Field$Mode]))

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
  TableInfo
  (to-clojure [x] {:location           (.location x)
                   :friendly-name      (.friendlyName x)
                   :description        (.description x)
                   :bytes              (.numBytes x)
                   :rows               (.numRows x)
                   :creation-time      (.creationTime x)
                   :expiration-time    (.expirationTime x)
                   :last-modified-time (.lastModifiedTime x)
                   :table-id           (to-clojure (.tableId x))}))

(defn service
  ([] (.service (BigQueryOptions/defaultInstance))))

(defn datasets [service]
  (let [it (-> service
               (.listDatasets (into-array BigQuery$DatasetListOption []))
               (.iterateAll))]
    (map to-clojure (iterator-seq it))))

(defn tables [service {:keys [project-id dataset-id] :as dataset}]
  (let [it (-> service
               (.listTables (DatasetId/of project-id dataset-id)
                            (into-array BigQuery$TableListOption []))
               (.iterateAll))]
    (map to-clojure (iterator-seq it))))

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

(defn- mkfield [{:keys [name type description mode fields]}]
  (let [field-type (condp = type
                     :bool      (Field$Type/bool)
                     :float     (Field$Type/floatingPoint)
                     :integer   (Field$Type/integer)
                     :string    (Field$Type/string)
                     :timestamp (Field$Type/timestamp)
                     :record    (Field$Type/record (map mkfield fields)))
        builder    (Field/builder name field-type)
        field-mode ({:nullable  (Field$Mode/NULLABLE)
                     :repeated  (Field$Mode/REPEATED)
                     :required  (Field$Mode/REQUIRED)} (or mode :nullable))]
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
  [service {:keys [project-id dataset-id table-id]} fields]
  (let [builder (TableInfo/builder (TableId/of project-id dataset-id table-id)
                                   (mkschema fields))
        table-info (.build builder)]
    (to-clojure (.create service table-info (into-array BigQuery$TableOption [])))))
