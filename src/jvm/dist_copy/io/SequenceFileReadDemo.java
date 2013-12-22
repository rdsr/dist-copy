package dist_copy.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        final String uri = "hdfs://localhost:8020//tmp/hadoop-yarn/staging/135/splits.info.seq";
        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(URI.create(uri), conf);
        final Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            final Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            final Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                final String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                System.out.println();
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
