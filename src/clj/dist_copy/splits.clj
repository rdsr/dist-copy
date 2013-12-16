(ns dist-copy.splits
  (:require [clojure.string :as s])
  (:import [java.io IOException]
           [java.util HashMap HashSet LinkedList Iterator]
           [org.apache.hadoop.net NodeBase]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path PathFilter LocatedFileStatus RemoteIterator]
           [org.apache.hadoop.util StringUtils]))

;; nothing works yet, just checking in for safe keeping

(defn input-paths
  [conf]
  {:pre (nil? (.get conf "dist.copy.input.paths"))} 
  (let [dirs (.get conf "dist.copy.input.paths")]
    (for [dir (s/split dirs #", *")]
      (-> dir StringUtils/unEscapeString Path.))))


(defn glob-status-for-path [conf path path-filter]
  (let [fs (.getFileSystem path conf)
        r (.globStatus fs path path-filter)]
    (if (nil? r)
      (throw (IOException. (str "Input path: " path " does not exist.")))
      r)))

  
(defn glob-status [conf paths path-filter]
  (mapcat #(glob-status-for-path conf % path-filter) paths))


(defn remove-redundant-files
  [conf file-statuses]
  (letfn [(components  
            [path]
            (take-while 
              (complement nil?) 
              (iterate #(.getParent %) path)))]
     (mapcat (fn [path] 
               (-> path (.getFileSystem conf) (.listStatus path)))
             (reduce (fn [hs path]
                       (if (some hs (components path))
                         hs
                         (conj hs path)))
                     #{}
                     (sort (map #(.getPath %) file-statuses))))))
  

(defn remote-files-seq [conf dir-status]
  (let [path (.getPath dir-status)
        fs (.getFileSystem path conf)
        remote-iter (.listLocatedStatus fs path)]
    (letfn [(iter-seq 
              [iter]
              (lazy-seq 
                (if (.hasNext iter)
                  (cons (.next iter) (iter-seq iter))
                  nil)))]
      (iter-seq remote-iter))))

  
(defn list-status [conf]
  (let [paths (input-paths conf)]
    (letfn [(_list-status_ 
              [file-statuses]
              (mapcat (fn [file-status]
                        (if (.isDirectory file-status)
                          (_list-status_ (remote-files-seq conf file-status)) 
                          [file-status]))
                      file-statuses))]
      (_list-status_
        (remove-redundant-files
          conf
          (glob-status conf paths
                       ;; todo add hidden filter
                       (reify PathFilter (accept [_ _] true))))))))


(defn file-blocks [conf file-status]
  (letfn [(block [_b] 
                 {:p (.getPath file-status) 
                  :o (.getOffset _b) 
                  :l (.getLength _b)
                  :h (-> _b .getHosts seq)
                  :r (map (fn [tp] 
                            (-> tp NodeBase. .getNetworkLocation)) 
                          (.getTopologyPaths _b))})]
    (let [fs (-> file-status .getPath (.getFileSystem conf))]
      (if (instance? LocatedFileStatus file-status)
        (map block (.getBlockLocations file-status))
        (map block (.getFileBlockLocations fs file-status 0 (.getLen file-status)))))))


(defn all-blocks
  [conf]
  (mapcat (partial file-blocks conf) (list-status conf)))

;(def conf (Configuration.))
;(.set conf "dist.copy.input.paths" "/tmp/f1, /*")
;(input-paths conf)
;(all-blocks conf)


(defn host->blocks 
  "Groups blocks by hosts. Note a block could be present
under multiple hosts (hdfs replication). Returns total
size of all data, and a map of host->blocks"
  [conf]
  (let [m (HashMap.)]
    ;; group by hosts
    (doseq [block (all-blocks conf) host (:h block)]
      (.put m :size (+ (get m :size 0) (:l block)))
      (if (contains? m host)
        (.offer (get m host) block)
        (.put m host (doto (LinkedList.) (.offer block)))))
    [(.remove m :size) m]))
    

;(defn generate-splits
;  [conf]
;  (let [[data-size host->blocks] (host->blocks conf)
;        split-size (compute-split-size conf data-size) 
;        rack->blocks (HashMap.)
;        blocks-used  (HashSet.)
;        [create-split close-file] (fn []
;                                    (let [f (create-listings-file conf)]
;                                      [(fn [blocks]
;                                         (doseq [b blocks]
;                                           (.add blocks-used b)
;                                           (.write f b)))
;                                       (fn []
;                                         (.close f))]))]
;    
;    (letfn [(group-by-rack 
;              [blocks]
;              (doseq [block blocks rack (:r block)]
;                (if (contains? rack->blocks rack)
;                  (.offer (get rack->blocks rack) block)
;                  (.put rack->blocks rack (doto (LinkedList.) (.offer block))))))
;            
;            (blocks-to-form-split 
;              [blocks]
;              (loop [total-size 0  
;                     [b bs] (remove #(contains? (dissoc % :h :r)) blocks)
;                     acc []]
;                (cond
;                  (nil? b) [:not-enough acc]
;                  (>= total-size split-size) acc
;                  :else (+ total-size (:l b)) bs (conj acc b))))
;                                                                    
;            (generate-splits-i 
;              [key->blocks all-keys on-not-enough-blocks]
;              (while (not (empty? key->blocks))
;                (let [key (all-keys (rand-int (count keys)))]
;                   (let [blocks (blocks-to-form-split (get key->blocks key))]
;                     (if (not= (blocks 0) :not-enough)
;                       (create-split blocks)
;                       (on-not-enough-blocks (blocks 1)))))))]
;
;      (generate-splits-i host->blocks
;                         (vec (keys host->blocks))
;                         group-by-rack)
;                           
;      (generate-splits-i racks->blocks
;                         (vec (keys rack->blocks))
;                         (fn [blocks] (create-split blocks))))))


