(ns dist-copy.client
  (:require [clojure.pprint :as pp])
  (:use [dist-copy.constants :as c])
  (:import [org.apache.hadoop.yarn.conf YarnConfiguration]
           [org.apache.hadoop.yarn.client.api YarnClient]
           [org.apache.hadoop.yarn.api.records QueueInfo 
            NodeState NodeReport QueueUserACLInfo QueueACL]))


(defn- queue-info 
  [^YarnClient yc]
  (let [queue (-> yc .getConfig (.get c/queue "default"))
        ^QueueInfo qi (.getQueueInfo yc queue)] 
    {:name (.getQueueName qi)
     :current-capacity (.getCurrentCapacity qi)
     :max-capacity (.getMaximumCapacity qi)
     :applications-count (-> qi .getApplications .size)
     :child-queues-count (-> qi .getChildQueues .size)}))


(defn- nodes-info
  [^YarnClient yc]
  (map (fn [^NodeReport node]
         {:id (.getNodeId node)
          :address (.getHttpAddress node)
          :rack (.getRackName node)
          :containers (.getNumContainers node)})
       (.getNodeReports yc (into-array [NodeState/RUNNING]))))


(defn- acls-info
  [yc]
  (for [^QueueUserACLInfo acl-info (.getQueueAclsInfo yc) 
        ^QueueACL user-acl (.getUserAcls acl-info)]
    [:queue (.getQueueName acl-info)
     :user  (.name user-acl)]))
                                          

(defn cluster-reports 
  [^YarnClient yc]
  {:node-managers (-> yc .getYarnClusterMetrics .getNumNodeManagers)
   :nodes (nodes-info yc)
   :queue-info (queue-info yc)
   :acls-info (acls-info yc)})
   

(defn print-cluster-reports
  [yc]
  (pp/pprint (cluster-reports yc)))

(defn- yarn-client
  ^YarnClient [^YarnConfiguration conf]
  (doto (YarnClient/createYarnClient)
    (.init conf)
    (.start)))

(defn- run [yc])

(defn copy 
  [& kvargs]
  (let [conf (YarnConfiguration.)
        kvs (apply hash-map kvargs)
        yc (yarn-client conf)]
    (print-cluster-reports yc)
    (run yc)))
    

;(:app-name "" :priority "" :queue "" :am-memory "")
           
;(copy)