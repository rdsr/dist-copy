(ns dist-copy.reports
  (:require [dist-copy.constants :as c])
  (:import [java.io File] 
           [org.apache.hadoop.security UserGroupInformation Credentials]
           [org.apache.hadoop.yarn.conf YarnConfiguration]
           [org.apache.hadoop.yarn.client.api YarnClient]
           [org.apache.hadoop.yarn.api ApplicationConstants ApplicationConstants$Environment] 
           [org.apache.hadoop.yarn.api.records QueueInfo
            NodeState NodeReport QueueUserACLInfo QueueACL]))

(defn- queue-info
  [^YarnClient yc]
  (let [queue (-> yc .getConfig (.get c/queue c/default-queue))
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
  [^YarnClient yc]
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