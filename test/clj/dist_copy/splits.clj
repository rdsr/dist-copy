(ns dist-copy.splits
  (:use [clojure.test]
        [dist-copy.splits])
  (:import [java.io File]
           [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.hdfs MiniDFSCluster]))


(defn- create-cluster [conf]
  (-> conf
      org.apache.hadoop.hdfs.MiniDFSCluster$Builder.
      .build))

(defn- destroy-cluster [cluster]
  (when (.isClusterUp cluster)
    (.shutdown cluster)))

;(deftest 
  ;(testing "splits file creation"
;   (is (= 0 1))))





