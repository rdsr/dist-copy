(ns dist-copy.splits
  (:require [clojure.string :as s])
  (:import [java.io IOException]
           [java.util HashMap HashSet LinkedList]
           [org.apache.hadoop.net NodeBase]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path PathFilter LocatedFileStatus]
           [org.apache.hadoop.util StringUtils]))

;; nothing works yet, just checking in for safe keeping

(defn input-paths [conf]
  (let [dirs (.get conf "dist.copy.input.paths", "")]
    (map #(StringUtils/unEscapeString %) (s/split dirs #","))))


(defn glob-status-for-path [conf dir path-filter]
  (let [path (Path. dir)
        fs (.getFileSystem path conf)
        r (.globStatus fs path path-filter)]
    (if (nil? r)
      (throw (IOException. (str "Input path: " dir " does not exist.")))
      r)))

  
(defn glob-status-for-paths [conf dirs path-filter]
  (mapcat #(glob-status-for-path conf % path-filter) dirs))


(defn list-status [conf]
  (let [paths (input-paths conf)]
    (mapcat (fn [stat]
              (if (.isDirectory stat)
                (iterator-seq (.listLocatedStatus (-> stat .getPath (.getFileSystem conf)) (.getPath stat)))
                [stat]))
            (glob-status-for-paths conf 
                                   paths 
                                   (reify PathFilter (accept [_ _] true))))))

(def conf (Configuration.))
(.set conf "dist.copy.input.paths" "/tmp,/tmp/f1")
(input-paths conf)
(seq (glob-status-for-path conf "/tmp" (reify PathFilter (accept [_ _] true))))
(list-status conf)


(defn file-blocks [file-status conf]
  (letfn [(block [_b] 
                 {:p (.getPath file-status) 
                  :o (.getOffset _b) 
                  :l (.getLength _b)
                  :h (.getHosts _b)
                  :r (map (fn [tp] 
                            (-> tp NodeBase. .getNetworkLocation)) 
                          (.getTopologyPaths _b))})]
    (let [fs (-> file-status .getPath (.getFileSystem conf))]
      (if (instance? LocatedFileStatus file-status)
        (map block (.getBlockLocations file-status))
        (map block (.getFileBlockLocations fs file-status 0 (.length file-status)))))))


(defn all-blocks
  [conf]
  (mapcat file-blocks (list-status conf)))


(defn host->blocks 
  "Groups blocks by hosts. Note a block could be present
under multiple hosts (hdfs replication). Returns total
size of all data, and a map of host->blocks"
  [conf]
  (let [m (HashMap.)]
    ;; group by hosts
    (doseq [block (all-blocks conf) host (:h b)]
      (.put m (+ (get m :size 0) (:l block)))
      (if (contains? m host)
        (.offer (get m host) block)
        (.put m host (doto (LinkedList.) (.offer block)))))
    [(.remove m :size) m]))
    

(defn generate-splits
  [conf]
  (let [[data-size host->blocks] (host->blocks conf)
        split-size (compute-split-size conf data-size) 
        rack->blocks (HashMap.)
        blocks-used  (HashSet.)
        [create-split close-file] (fn []
                                    (let [f (create-listings-file conf)]
                                      [(fn [blocks]
                                         (doseq [b blocks]
                                           (.add blocks-used b)
                                           (.write f b)))
                                       (fn []
                                         (.close f))]))]
    
    (letfn [(group-by-rack 
              [blocks]
              (doseq [block blocks rack (:r block)]
                (if (contains? rack->blocks rack)
                  (.offer (get rack->blocks rack) block)
                  (.put rack->blocks rack (doto (LinkedList.) (.offer block))))))
            
            (blocks-to-form-split 
              [blocks]
              (loop [total-size 0  
                     [b bs] (remove #(contains? (dissoc % :h :r)) blocks)
                     acc []]
                (cond
                  (nil? b) [:not-enough acc]
                  (>= total-size split-size) acc
                  :else (+ total-size (:l b)) bs (conj acc b))))
                                                                    
            (generate-splits-i 
              [key->blocks all-keys on-not-enough-blocks]
              (while (not (empty? key->blocks))
                (let [key (all-keys (rand-int (count keys)))]
                   (let [blocks (blocks-to-form-split (get key->blocks key))]
                     (if (not= (blocks 0) :not-enough)
                       (create-split blocks)
                       (on-not-enough-blocks (blocks 1)))))))]

      (generate-splits-i host->blocks
                         (vec (keys host->blocks))
                         group-by-rack)
                           
      (generate-splits-i racks->blocks
                         (vec (keys rack->blocks))
                         (fn [blocks] (create-split blocks))))))



