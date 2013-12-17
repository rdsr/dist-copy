package dist_copy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class Split implements Writable {
    // Currently may contain just a single location (host/rack)
    private String[] locations;
    private Chunk[] chunks;

    public Split() {}

    public Split(Collection<Chunk> _chunks) {
        chunks = new Chunk[_chunks.size()];
        int i = 0;
        for (Chunk c : _chunks) {
            chunks[i] = c;
        }
    }
    
    

    @Override
    public void readFields(DataInput in) throws IOException {
        int sz = in.readInt();
        chunks = new Chunk[sz];
        for (int i = 0; i < sz; i++) {
            chunks[i] = new Chunk();
            chunks[i].readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(chunks.length);
        for (int i = 0; i < chunks.length; i++) {
            chunks[i].write(out);
        }
    }
}
