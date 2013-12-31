(ns dist-copy.util
  (:import [java.io File FileOutputStream BufferedOutputStream]
           [java.util.zip ZipOutputStream ZipEntry] 
           [org.apache.hadoop.yarn.util Records]))


(defn mk-record [clazz]
  (Records/newRecord class))


(defn- jar-file [resource]
  (-> resource .getPath (.split "!") first))


(defn- create-zip [base-dir]
  (let [zip (str (System/getProperty "java.io.tmpdir") "/dist-copy.zip")]
    (with-open [os (-> zip FileOutputStream. BufferedOutputStream. ZipOutputStream.)]
      (doseq [f (-> base-dir File. file-seq)]
        (when (.isFile f)
          (.putNextEntry os 
            (ZipEntry. (-> f str (.replace  base-dir "")))))))
    zip))
    

(defn- zip-file [resource rel-dir]
  (let [rel-dir (if (.startsWith rel-dir "/") 
                  (subs rel-dir 1)
                  rel-dir)
        dir (-> resource .getPath)]
    (create-zip 
      (.substring dir 0 (.indexOf dir rel-dir)))))
              

(defn archive [clazz]
  (let [name (str "/" 
                  (.. clazz getName (replace "." "/")) 
                  ".class")
        resource (.getResource clazz name)]
    (if (= "jar" (.getProtocol resource))
      (jar-file resource)
      (zip-file resource name))))
