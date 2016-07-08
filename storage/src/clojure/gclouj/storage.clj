(ns gclouj.storage
  (:import [com.google.cloud AuthCredentials]
           [com.google.cloud.storage StorageOptions Storage$BlobListOption Storage$BucketListOption Bucket Blob BlobId Storage$SignUrlOption Storage$BlobSourceOption BlobInfo Storage$BlobWriteOption]
           [gclouj StorageOptionsFactory]
           [java.util.concurrent TimeUnit]
           [java.nio.channels Channels])
  (:require [clojure.java.io :as io]
            [clj-time.coerce :as tc]
            [clj-time.core :as tcc]))


(defprotocol ToClojure
  (to-clojure [x]))

(extend-protocol ToClojure
  BlobId
  (to-clojure [id] {:bucket (.bucket id)
                    :name   (.name id)})
  BlobInfo
  (to-clojure [blob] {:id                  (to-clojure (.blobId blob))
                      :cache-control       (.cacheControl blob)
                      :content-disposition (.contentDisposition blob)
                      :content-encoding    (.contentEncoding blob)
                      :content-language    (.contentLanguage blob)
                      :content-type        (.contentType blob)
                      :crc32c              (.crc32c blob)
                      :generation          (.generation blob)
                      :md5                 (.md5 blob)
                      :metadata            (.metadata blob)
                      :name                (.name blob)
                      :bytes               (.size blob)
                      :media-link          (.mediaLink blob)
                      :updated-at          (tc/from-long (.updateTime blob))
                      :directory           (.isDirectory blob)})
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


(defn buckets [service]
  (->> (.iterateAll (.list service (into-array Storage$BucketListOption [])))
       (iterator-seq)
       (map to-clojure)))

(defn- blobs* [service bucket options]
  (->> (.iterateAll (.list service bucket (into-array Storage$BlobListOption options)))
       (iterator-seq)
       (map to-clojure)))

(defn blobs
  ([service bucket]
   (blobs* service bucket []))
  ([service bucket prefix]
   (blobs* service bucket [(Storage$BlobListOption/prefix prefix)])))


(defn signed-url
  "Generates a signed URL for the blob, valid for the length of the
  period specified. Period should be a Joda
  period,e.g.: (clj-time.core/minutes 5)"
  [service {:keys [bucket name] :as blob-id} period]
  (when-let [blob (.get service (BlobId/of bucket name))]
    (.signUrl blob (tcc/in-seconds period) TimeUnit/SECONDS (into-array Storage$SignUrlOption []))))

(defn content-stream
  [service {:keys [bucket name] :as blob-id}]
  (when-let [reader (.reader service (BlobId/of bucket name) (into-array Storage$BlobSourceOption []))]
    (Channels/newInputStream reader)))


(defn- blob-info [{:keys [bucket name cache-control content-disposition content-encoding content-language content-type metadata]}]
  (let [b (BlobInfo/builder (BlobId/of bucket name))]
    (when cache-control
      (.cacheControl b cache-control))
    (when content-disposition
      (.contentDisposition b content-disposition))
    (when content-encoding
      (.contentEncoding b content-encoding))
    (when content-language
      (.contentLanguage b content-language))
    (when content-type
      (.contentType b content-type))
    (when metadata
      (.metadata b metadata))
    (.build b)))

(defn- write-options [options]
  (let [opts {:does-not-exist (Storage$BlobWriteOption/doesNotExist)}]
    (->> options (map opts) (into-array Storage$BlobWriteOption))))

(defn create-blob
  "Creates a blob. blob should be a map with at least :bucket
  and :name (for other keys, see blob-info fn).
  Will succeed even if blob already exists- to fail if the blob already exists, "
  [service blob input-stream & opts]
  (to-clojure (.create service (blob-info blob) input-stream (write-options opts))))


(defn delete-blob
  "Deletes a blob. Returns boolean indicating if it was deleted
  successfully."
  [service {:keys [bucket name] :as blob-id}]
  (.delete service (BlobId/of bucket name)))
