(ns dist-copy.splits
  (:require [clojure.string :as s])
  (:import [java.io IOException]
           [java.util HashMap HashSet LinkedList Iterator]
           [org.apache.hadoop.net NodeBase]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path PathFilter LocatedFileStatus RemoteIterator]
           [org.apache.hadoop.util StringUtils]))

;; nothing works yet, just checking in for safe keeping

(defn- input-paths
   "Returns, as a list, the user specified input paths.
Paths are comma separated, could contain files and directories,
path can also be glob patterns. 

i.e: '/tmp/f*.tgz, /tmp/dir2'" 
  [conf]
  (let [dirs (.get conf "dist.copy.input.paths")]
    (for [dir (s/split dirs #", *")]
      (-> dir StringUtils/unEscapeString Path.))))


(defn- glob-status-for-path
  "Returns all paths which match the glob 
pattern and satisfy the path filter."
  [conf path path-filter]
  (let [fs (.getFileSystem path conf)
        r (.globStatus fs path path-filter)]
    (if (nil? r)
      (throw (IOException. (str "Input path: " path " does not exist.")))
      r)))

  
(defn- glob-status [conf paths path-filter]
  "Returns all paths which match the glob-patterns
and also satisfy the path filter"
  (mapcat #(glob-status-for-path conf % path-filter) paths))


(defn- path-ancestors 
  "Given a path returns all it's ancestors including
the path itself /a/b/c --> /a/b/c /a/b /a /"
  [path]
  (take-while (complement nil?)  
              (iterate #(.getParent %) path)))


(defn- remove-redundant-files
  "Removes redundant files from the input so that we 
don't needlessly process them again. e.g. If the
input contains /tmp and /tmp/a1, /tmp/a1 is removed
from the result"
  [conf file-statuses]
  (mapcat (fn [path]
            (-> path 
              (.getFileSystem conf) 
              (.listStatus path)))
          (reduce (fn [hs path]
                    (if (some hs (path-ancestors path))
                      hs
                      (conj hs path)))
                  #{}
                  (sort (map #(.getPath %) file-statuses)))))
  

(defn- remote-iterator-seq
  "Returns a seq on the remote file-status iterator.
The remote iterator is received when we list the 
contents of an hdfs directory"
  [remote-iterator]
  (lazy-seq 
    (if (.hasNext iter)
      (cons (.next iter) (remote-iterator-seq iter))
      nil)))


(defn- remote-files-seq [conf dir-status]
  "Lists all files under the specified hdfs directoy"
  (let [path (.getPath dir-status)
        fs (.getFileSystem path conf)
        remote-iter (.listLocatedStatus fs path)]
    (remote-iterator-seq remote-iter)))


(defn- list-status-recursively
  "Recursively list all files under the dirs specified"
  [file-statuses]
  (mapcat (fn [file-status]
            (if (.isDirectory file-status)
              (list-status-recursively 
                (remote-files-seq conf file-status)) 
              [file-status]))
          file-statuses)
  
(defn- list-status [conf]
  "Recursively list all files under 'dist.copy.input.paths'. If
the input contains files, those files are also listed. Also removes
duplicate files from further processing."
  (let [paths (input-paths conf)
        hidden-files-filter 
        (reify PathFilter
          (accept [_ p]
            (let [name (.getName p)]
              (not (or (.startsWith name "_") (.startsWith name "."))))))] 
      (list-status-recursively
        (remove-redundant-files
          conf
          (glob-status conf paths hidden-files-filter))))))
                    

(defn- file-blocks [conf file-status]
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
        (map block 
             (.getBlockLocations file-status))
        (map block 
             (.getFileBlockLocations fs 
               file-status 0 (.getLen file-status)))))))


(defn all-blocks
  [conf]
  (mapcat (partial file-blocks conf) (list-status conf)))

;(def conf (Configuration.))
;(.set conf "dist.copy.input.paths" "/tmp/, /**/a*")
;(input-paths conf)
;(all-blocks conf)
;(host->blocks conf)

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
        (.put m host (doto (LinkedList.) 
                       (.offer block)))))
    [(.remove m :size) m]))


(defn blocks->chunks
  [blocks]h
  (let [path->blocks (HashMap.)]
    (doseq [{:keys [p o l] :as b} blocks]
      (if (contains? path->blocks p)
        (.offer (get path->blocks p) [o l])
        (.put path->blocks p (doto (PriorityQueue. 100 compare) (.offer [o l]))))

(defn- compute-split-size
  [conf data-size]
  (max (.getInt conf "dist.copy.min.split.size", (* 128 1024 1024) 
     (/ data-size (.getInt conf "dist.copy.num.tasks")))))


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


