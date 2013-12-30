(ns dist-copy.client
  (:require [clojure.pprint :as pp])
  (:use [clojure.string :only (join)] 
        [dist-copy.util] 
        [dist-copy.constants :as c])
  (:import [java.io File] 
           [org.apache.hadoop.yarn.conf YarnConfiguration]
           [org.apache.hadoop.yarn.client.api YarnClient]
           [org.apache.hadoop.yarn.api ApplicationConstants ApplicationConstants$Environment] 
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


(defn print-cluster-reports
  [yc] (pp/pprint (cluster-reports yc)))


(defn- mk-conf
  [kvs] (let [conf (YarnConfiguration.)]
          (doseq [[k v] kvs]
            (.set conf (name k) (name v)))
          conf))
  


(defn- yarn-client
  ^YarnClient [^YarnConfiguration conf]
  (doto (YarnClient/createYarnClient)
    (.init conf)
    (.start)))


(defn- app-classpath
  [conf]
  ;; todo Add log4j
  (join
    File/pathSeparatorChar
    (map #(.trim %)  
         (concat ["$CLASSPATH" "."]  
                 (.getStrings conf  
                   YarnConfiguration/YARN_APPLICATION_CLASSPATH 
                   YarnConfiguration/DEFAULT_YARN_APPLICATION_CLASSPATH)))))

(defn- mk-am-env
  [conf]
  ;; Currently just set the classpath
  {"CLASSPATH" (app-classpath conf)})   

(defn- mk-am-cmd
  [] (join 
       " "
       ["$JAVA_HOME/bin/java"
       "-Xmx" 
       (str (.getInt conf "dist.copy.am.memory.mb" 512) "m") 
       (str "1>" ApplicationConstants/LOG_DIR_EXPANSION_VAR "dist-copy.stdout")
       (str "2>" ApplicationConstants/LOG_DIR_EXPANSION_VAR "dist-copy.stderr")]))
        

(defn- mk-AM-container
  "Setup container launch context for Application Master"
  []
  (let [amc (mk-record ContainerLaunchContext)
        local-resources {:jar (am-jar conf)}]
    (doto amc
      (.setEnv (mk-am-env conf))
      (.setCommands [(mk-am-cmd)])
      (.setTokens (security-tokens))))
    
    
                         
    
  ;; local-resource
  ;; log file
  ;; classpath
  ;; command
  )

(defn- mk-AM
  "Setup Application Master"
  [^YarnClientApplication app ^YarnConfiguration conf]
  (let [appContext (.getApplicationSubmissionContext app)]
    (.setApplicationName appContext (conf))
  ;; Create application submission context
  ;; Create launch container
  ;; security tokens
  )

(defn- run [yc]
  (let [conf (.getConfig yc)
        app (.createApplication yc)
        app-response (.getNewApplicationRespone app)]
    ;; Todo cap am-memory to max container memory
    (mk-AM app conf)

(defn copy
  [& kvargs]
  (let [conf (mk-conf kvargs)
        yc (yarn-client conf)]
    (print-cluster-reports yc)
    (run yc)
    (.stop yc)))


;(:app-name "" :priority "" :queue "" :am-memory "")

;(copy)
