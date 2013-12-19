(ns dist-copy.splits
  (:require [clojure.string :as s])
  (:import [java.io IOException]
           [java.util HashMap HashSet LinkedList Iterator]
           [org.apache.hadoop.net NodeBase]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path PathFilter LocatedFileStatus RemoteIterator]
           [org.apache.hadoop.util StringUtils]))


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

  
(defn- glob-status 
  "Returns all paths which match the glob-patterns
and also satisfy the path filter"
  [conf paths path-filter]
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


(defn- remote-files-seq
  "Lists all files under the specified hdfs directoy"
  [conf dir-status]
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
          file-statuses))
  

(defn- list-status 
  "Recursively list all files under 'dist.copy.input.paths'. If
the input contains files, those files are also listed. Also removes
duplicate files from further processing."
  [conf]
  (let [paths (input-paths conf)
        hidden-files-filter 
        (reify PathFilter
          (accept [_ p]
            (let [name (.getName p)]
              (not (or (.startsWith name "_") (.startsWith name "."))))))] 
      (list-status-recursively
        (remove-redundant-files
          conf
          (glob-status 
            conf paths hidden-files-filter)))))
                    

(defn- file-blocks
  "Returns all blocks, as a map {:o offset, :p path, 
:l length, :h (hosts) :r (racks)}, for a given file" 
  [conf file-status]
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


(defn- all-blocks
  "Returns all blocks for each input file specified"
  [conf]
  (mapcat (partial file-blocks conf) (list-status conf)))


(defn- blocks->chunks
  "Converts a coll of blocks to a coll of @dist_copy.io.Chunks"
  [blocks]
  (for [[path grouped-blocks] (group-by :p blocks)]
    ;TODO: grouped-blocks can contain consecutive blocks, optimise this later
    (Chunk. path (:l grouped-blocks) (map :o grouped-blocks)))) 


(defn- compute-split-size
  [conf data-size]
  (max (.getInt conf "dist.copy.min.split.size", (* 128 1024 1024) 
     (/ data-size (.getInt conf "dist.copy.num.tasks")))))


(defn- total-size
  [blocks]
  (reduce 
    (fn [sum b]
      (+ sum (:l b))) blocks))


;; higly imperative code follows :(
;; Todo: maybe try transients on my nested maps here

(defn- create-split
  [seqfile-writer blocks]
  (.append writer (NullWritable.) (blocks->split blocks)))


(defn- create-splits
  [k-blocks ks create-split enough-blocks not-enough-blocks]
  (while (not (empty? k-blocks))
    (let [k (rand-nth ks)
          [enough? blocks] (enough-blocks (m k))]
      (if enough?
        (create-split blocks)
        (not-enough-blocks blocks)))))
        

(defn- enough-blocks 
  [split-size blocks]
  (loop [sz 0 acc (LinkedList.)]
    (cond
      (>= sz split-size) [true acc]
      (empty? blocks) [false acc]
      :else (let [b (.poll blocks)]                 
              (recur (+ sz (:l b)) (.offer acc b))))))


(defn- not-enough-blocks
  [rack-blocks blocks]
  (doseq [b blocks]
    (let [k (:r b)]
      (if (contains? rack-blocks k))
      (.put rack-blocks 
        k (.offer (rack-blocks k) b)))))


(defn- group-blocks-by 
  ([f blocks]
    (group-blocks (HashMap.) f blocks))
  ([m f blocks]
    (doseq [b blocks]
      (let [k (f b)]
        (if (contains? m k))
        (.put m 
          k (.offer (m k) b))))))


(defn- splits-file-path
  [conf]
  (Path. ; TODO: check this, it may not be correct 
    (str (.get conf "yarn.app.mapreduce.am.staging-dir")
         "/" (.get conf "yarn.app.attempt.id") "/splits.info.seq")))


(defn- seqfile-writer
  [conf]
  (let [path (splits-file-path conf)
        fs (.getFileSystem path conf)]
    (SequeneFile/createWriter 
      fs conf path NullWritable Split)))
  

(defn generate-splits
  [conf]
  (let [blocks      (all-blocks conf)
        host-blocks (group-blocks-by :h blocks)
        split-size  (compute-split-size conf (total-size blocks))]
    (with-open [sf-wr (seqfile-writer conf)]
      (let [create-split  (partial create-split sf-wr)
            enough-blocks (partial enough-blocks split-size)
            rack-blocks   (HashMap.)
            not-enough-blocks (partial group-blocks-by rack-blocks :r)]
        (create-splits 
          host-blocks create-split enough-blocks not-enough-blocks)            
        (create-splits 
          racks-blocks create-split enough-blocks create-split)))))


;(def conf (Configuration.))
;(.set conf "dist.copy.input.paths" "/tmp/, /**/a*")
;(input-paths conf)
;(all-blocks conf)
;(host->blocks conf)
