(ns dist-copy.splits
  (:require [clojure.string :as s])
  (:import [dist_copy.io Split Chunk Block]
           [java.io IOException]
           [java.util HashMap HashSet LinkedList Iterator]
           [org.apache.hadoop.io NullWritable SequenceFile]
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
  [remote-iter]
  (lazy-seq
    (if (.hasNext remote-iter)
      (cons (.next remote-iter) (remote-iterator-seq remote-iter))
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
  [conf file-statuses]
  (mapcat (fn [file-status]
            (if (.isDirectory file-status)
              (list-status-recursively
                conf
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
      conf
      (remove-redundant-files
        conf
        (glob-status
          conf paths hidden-files-filter)))))


(defn- file-blocks
  "Returns all blocks, as a map {:o offset, :p path,
:l length, :h (hosts) :r (racks)}, for a given file"
  [conf file-status]
  (let [path (.getPath file-status)
        fs   (.getFileSystem path conf)]
  (letfn [(racks [_b] 
                 (map (fn [tp]
                        (-> tp NodeBase. .getNetworkLocation set))
                      (.getTopologyPaths _b)))
          (block [_b]
                 {:p path
                  :o (.getOffset _b)
                  :l (.getLength _b)
                  :h (-> _b .getHosts set)
                  :r (racks _b)})]
      (map block
           (if (instance? LocatedFileStatus file-status)
             (.getBlockLocations file-status)
             (.getFileBlockLocations 
               fs file-status 0 (.getLen file-status)))))))
               

(defn- all-blocks
  "Returns all blocks for each input file specified"
  [conf]
  (mapcat (partial file-blocks conf) (list-status conf)))


(defn- compute-split-size
  [conf data-size]
  (let [min-split-size (.get conf "dfs.namenode.fs-limits.min-block-size")
        split-size (.get conf "dist.copy.split.size")]
    (min data-size (max min-split-size split-size))))

    
(defn- total-size
  [blocks]
  (reduce
    (fn [sum b]
      (+ sum (:l b))) 0 blocks))


;; ----------------------------
;; higly imperative code follows :(
;; Todo: maybe try transients on my nested maps here

(defn- group-blocks-by
  "Groups blocks based on the output of the function f. 
(f block) is expected to be a coll or it is converted 
to one. This fn is mainly used to group blocks by either
rack or host. Since a block may be present in multiple
hosts/racks, each host/rack gets a copy of the block"
  ([f blocks]
    (group-blocks-by (HashMap.) f blocks))
  ([m f blocks]
    (letfn [(group-fn 
              [block]
              (let [r (f block)]
                (if (coll? r) r [r])))]
      (doseq [b blocks k (group-fn b)]
        (when (not (contains? m k))
          (.put m k (LinkedList.)))
        (.put m k (doto (get m k) (.offer b))))
      m)))


(defn- blocks->chunks
  "Converts a coll of blocks, grouped by host or rack, to a coll of @dist_copy.io.Chunks"
  [conf node blocks]
  (let [block (first blocks)
        [host rack] (if (some #(= % node) (:h block))
                      [node nil]
                      [nil node])]
    (for [[path grouped-blocks] 
          (group-blocks-by :p blocks)] 
      ;; TODO: grouped-blocks can contain consecutive blocks, optimise this later
      (Chunk. path
              host 
              rack
              (map (fn [{:keys [o l h r]}]
                     (Block. o l h r))
                   grouped-blocks)))))


(defn- create-split-fn
  [conf seqfile-writer used-blocks]
  (fn [node blocks]
    (.addAll used-blocks blocks)
    (.append seqfile-writer (NullWritable/get) (Split. (blocks->chunks conf node blocks)))))


(defn- create-splits
  "Keeps creating node (host/rack) local splits by randomly
choosing nodes. If there are enough blocks per node it creates 
a split, else those blocks are handled by 'not-enough-blocks'
Nodes are chosen randomly for proper split distribution."
  [node-blocks create-split enough-blocks not-enough-blocks]
  (let [nodes (vec (keys node-blocks))]
    (while (not (empty? node-blocks))
      (let [node (rand-nth nodes)
            blocks (get node-blocks node)
            [enough? blocks] (enough-blocks blocks)]
        (if enough? 
          (create-split node blocks)
          (if (empty? blocks)
            ;; No more blocks for node (host/rack), 
            ;; remove the key from the map 
            (.remove node-blocks node)
            (not-enough-blocks node blocks)))))))


(defn- enough-blocks-fn
  [split-size used-blocks]
  (fn [blocks]
    (loop [sz 0 acc (LinkedList.)]
      (cond
        (>= sz split-size) [true acc]
        (empty? blocks) [false acc]
        :else (let [b (.poll blocks)]
                (if (contains? used-blocks b)
                    (recur sz acc)
                    (recur (+ sz (:l b)) (doto acc (.offer b)))))))))


(defn- splits-file-path
  "Returns a path to where @dsit_copy.io.Splits will be written"
  [conf]
  (Path.
    (str (.get conf "yarn.app.mapreduce.am.staging-dir")
         "/" (.get conf "yarn.app.attempt.id") "/splits.info.seq")))


(defn- seqfile-writer
  "Each @dist_copy.io.Split will be written in a sequence file as 
a value. The key will be a NullWritable"
  [conf]
  (let [path (splits-file-path conf)
        fs (.getFileSystem path conf)]
    (SequenceFile/createWriter fs conf path NullWritable Split)))


(defn create-splits-file
  [conf]
  (let [blocks      (all-blocks conf)
        host-blocks (group-blocks-by :h blocks) ;; host-local blocks
        split-size  (compute-split-size conf (total-size blocks))]

    (with-open [sf-wr (seqfile-writer conf)]
      (let [used-blocks   (HashSet.) ;; To filter out blocks already used in some split
            rack-blocks   (HashMap.) ;; rack-local blocks
            create-split  (create-split-fn conf sf-wr used-blocks)
            enough-blocks (enough-blocks-fn split-size used-blocks)            
            not-enough-blocks (fn [_ blocks] 
                                ;; giving same signature to this fn as create-split, since 
                                ;; either of 'create-split' or this fn could be used to
                                ;; handle the case when we don't have >= split-size blocks
                                ;; see (create-splits rack-blocks ...) below
                                (group-blocks-by rack-blocks :r blocks))]

        ;; create host-local splits
        (create-splits
          host-blocks create-split enough-blocks not-enough-blocks) 
        ;; create rack-local splits
        (create-splits 
          rack-blocks create-split enough-blocks create-split))))) 
;; ----------------------------
