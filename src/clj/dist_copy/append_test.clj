(ns dist-copy.append-test
  (:use [clojure.java.io :as io])
  (:import [dist_copy.util BoundedInputStream]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileSystem Path]))

(defn- block-size 
  [conf path]
  (-> path (.getFileSystem conf) (.getFileStatus path) .getBlockSize))


(defn test-append
  [conf src dst sz]
  (let [fs (FileSystem/get conf)
        bsz (block-size conf src)]
    (dotimes [_ (/ sz bsz)]
      (with-open [is (-> fs (.open src) (BoundedInputStream. bsz))
                  os (.append fs dst)]
        (io/copy is os)))))


(defn create-empty-file
  [conf path]
  (with-open [_ (.create (FileSystem/get conf) path)]))


(def gb (* 1024 1024 1024))


(defn timeit
  [conf src dst sz]
  (time (create-empty-file conf dst) 
        (test-append conf src dst (* sz gb)))