(ns dist-copy.client
  (:require [clojure.pprint :as pp])
  (:use [clojure.string :only (join blank? trim)]
        [dist-copy.util]
        [dist-copy.reports]
        [dist-copy.constants :as c])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.io DataOutputBuffer]
           [org.apache.hadoop.security UserGroupInformation Credentials]
           [org.apache.hadoop.yarn.conf YarnConfiguration]
           [org.apache.hadoop.yarn.util ConverterUtils]
           [org.apache.hadoop.yarn.client.api YarnClient YarnClientApplication]
           [org.apache.hadoop.yarn.api.records ApplicationId ApplicationReport ApplicationSubmissionContext
            LocalResource LocalResourceType LocalResourceVisibility YarnApplicationState ContainerLaunchContext Resource]
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
  [^YarnConfiguration conf]
  ;; TODO Add log4j
  (join
    File/pathSeparatorChar
    (concat 
      ["$CLASSPATH" "."]
      (map #(trim %)
        (.getStrings conf
          YarnConfiguration/YARN_APPLICATION_CLASSPATH
          YarnConfiguration/DEFAULT_YARN_APPLICATION_CLASSPATH)))))

(defn- mk-AM-env
  [conf]
  ;; Currently just set the classpath
  {"CLASSPATH" (app-classpath conf)})


(defn- mk-AM-cmd
  [^YarnConfiguration conf]
  (join " "
        ["$JAVA_HOME/bin/java"
        "-Xmx"
        (str (.getInt conf c/am-memory 512) "m")
        (str "1>" ApplicationConstants/LOG_DIR_EXPANSION_VAR c/application-name ".stdout")
        (str "2>" ApplicationConstants/LOG_DIR_EXPANSION_VAR c/application-name ".stderr")]))


(defn- security-tokens
  ;; TODO test this out
  [^YarnConfiguration conf]
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
  [^YarnConfiguration conf]
  (let [fs (FileSystem/get conf)
        src-path (-> dist_copy.ApplicationMaster archive Path.)
        dst-path (Path. (join "/"
                              [(.getHomeDirectory fs)
                               (.get conf c/application-name)
                               (.get conf c/app-id)
                               "application-master.jar"]))
        dst-status (.getFileStatus fs dst-path)]
    (.copyFromLocalFile fs false true src-path dst-path)
    (LocalResource/newInstance
      (ConverterUtils/getYarnUrlFromPath dst-path)
      LocalResourceType/FILE
      LocalResourceVisibility/APPLICATION
      (.getLen dst-status)
      (.getModificationTime dst-status))))


(defn- local-resources
  [conf]
  ;; For now just the am jar, maybe add log4j
  ;; needs all the dependencies needed by AM (for instance: clojure.jar)
  {:application-master.jar (am-jar conf)})

(defn- setup-AM-launch-context
  "Setup container launch context for Application Master"
  [^YarnConfiguration conf]
  (let [^ContainerLaunchContext amc (mk-record ContainerLaunchContext)]
    (doto amc
      (.setLocalResources (local-resources conf))
      (.setEnvironment (mk-AM-env conf))
      (.setCommands [(mk-AM-cmd conf)])
      (.setTokens (security-tokens conf)))))


(defn- am-resource
  [^YarnConfiguration conf]
  (doto (mk-record Resource)
    (.setMemory (.get conf c/am-memory))))

(defn- mk-AM
  "Setup Application Master"
  ^ApplicationSubmissionContext [^YarnClientApplication app ^YarnConfiguration conf]
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
                (complement :finished) (repeatedly report))]
      (pp/pprint r)
      (Thread/sleep 1000)
    (report)))) ;; Print last finshed report


(defn- run
  [^YarnClient yc]
  (let [conf (.getConfig yc)
        app (.createApplication yc)
        app-submission-context (mk-AM app conf)]
    ; app-response (.getNewApplicationRespone app)]
    ;; TODO cap am-memory to max container memory
    (.submitApplication yc app-submission-context)
    (monitor yc (.getApplicationId app-submission-context))))


(defn copy
  [& kvargs]
  (let [conf (mk-conf kvargs)
        yc (yarn-client conf)]
    (print-cluster-reports yc)
    (run yc)
    (.stop yc)))
