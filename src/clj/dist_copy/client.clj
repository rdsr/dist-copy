(ns dist-copy.client
  (:require [clojure.pprint :as pp])
  (:use [clojure.string :only (join blank?)] 
        [dist-copy.util] 
        [dist-copy.constants :as c])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.io DataOutputBuffer]
           [org.apache.hadoop.security UserGroupInformation Credentials]
           [org.apache.hadoop.yarn.conf YarnConfiguration]
           [org.apache.hadoop.yarn.client.api YarnClient YarnClientApplication]
           [org.apache.hadoop.yarn.api.records ApplicationId ApplicationReport 
            YarnApplicationState ContainerLaunchContext Resource]
           [org.apache.hadoop.yarn.api ApplicationConstants ApplicationConstants$Environment]))


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
  ;; TODO Add log4j
  (join
    File/pathSeparatorChar
    (map #(.trim %)  
         (concat ["$CLASSPATH" "."]  
                 (.getStrings conf  
                   YarnConfiguration/YARN_APPLICATION_CLASSPATH 
                   YarnConfiguration/DEFAULT_YARN_APPLICATION_CLASSPATH)))))

(defn- mk-AM-env
  [conf]
  ;; Currently just set the classpath
  {"CLASSPATH" (app-classpath conf)})


(defn- mk-AM-cmd
  [conf] 
  (join " " 
        ["$JAVA_HOME/bin/java"
        "-Xmx"
        (str (.getInt conf c/am-memory 512) "m") 
        (str "1>" ApplicationConstants/LOG_DIR_EXPANSION_VAR c/app-name ".stdout")
        (str "2>" ApplicationConstants/LOG_DIR_EXPANSION_VAR c/app-name ".stderr")]))


(defn- security-tokens
  ;; TODO test this out
  [conf]
  (when (UserGroupInformation/isSecurityEnabled)
    (let [fs (FileSystem/get conf)
          creds (Credentials.)
          token-renewer (.get conf YarnConfiguration/RM_PRINCIPAL)]
      (assert (-> token-renewer blank? not) 
              "Can't get Master Kerberos principal for the RM to use as renewer")
      (.addDelegationTokens fs token-renewer creds)
      (let [dob (DataOutputBuffer.)]
        (.writeTokenStorageToStream creds dob)
        (ByteBuffer/wrap (.getData dob) 0 (.getLength dob))))))
  

(defn- am-jar
  [conf] (archive dist_copy.ApplicationMaster))

(defn- mk-AM-launch-context
  "Setup container launch context for Application Master"
  [conf]
  (let [amc (mk-record ContainerLaunchContext)
        local-resources {"application-master.jar" (am-jar conf)}]
    (doto amc
      (.setLocalResources local-resources)
      (.setEnv (mk-am-env conf))
      (.setCommands [(mk-am-cmd conf)])
      (.setTokens (security-tokens conf)))))
  

(defn- am-resource 
  [^YarnConfiguration conf]
  (doto (mk-record Resource)
    (.setMemory (.get conf c/am-memory))))

(defn- mk-AM
  "Setup Application Master"
  [^YarnClientApplication app ^YarnConfiguration conf]
  (doto (.getApplicationSubmissionContext app)
    (.setApplicationName (.get conf c/application-name))
    (.setAMContainerSpec (setup-AM-launch-context conf))
    ;; TODO Priority
    (.setQueue (.get conf c/queue c/default-queue))
    (.setResource (am-resource conf))))


(defn- monitor
  [^YarnClient yc ^ApplicationId app-id]
  (letfn [(report 
            [] (let [r (.getApplicationReport yc app-id)
                     app-state (.getYarnApplicationState r)]   
                 {:app-id           (.getId app-id)
                  :client->AM-token (.getClientToAMToken r)
                  :app-diagnostics  (.getDiagnostics r)
                  :app-master-host  (.getHost r)
                  :app-start-time   (.getStartTime r)
                  :yarn-app-state   app-state
                  :final-app-state  (.getFinalApplicationStatus r)
                  :app-tracking-url (.getTrackingUrl r)
                  :finished (or (= app-state YarnApplicationState/FINISHED)
                                (= app-state YarnApplicationState/KILLED))}))]
    (doseq [r (take-while 
                (complement :finished)
                (repeatedly report))]
      (pp/pprint r)
      (Thread/sleep 1000)
    (report)))) ;; Print last finshed report


(defn- run 
  [^YarnClient yc]
  (let [conf (.getConfig yc)
        app (.createApplication yc)]
        ;app-response (.getNewApplicationRespone app)]
    ;; TODO cap am-memory to max container memory
    (.submitApplication yc (mk-AM app conf))
    (monitor yc (.getApplicationId app))))
    

(defn copy
  [& kvargs]
  (let [conf (mk-conf kvargs)
        yc (yarn-client conf)]
    (print-cluster-reports yc)
    (run yc)
    (.stop yc)))