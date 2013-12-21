package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.io.Writable;

public class Split implements Writable {
    private Collection<Chunk> chunks;

    public Split() {}

    public Split(Collection<Chunk> chunks) {
        this.chunks = new ArrayList<Chunk>(chunks);
    }

    public Collection<Chunk> getChunks() {
        return Collections.unmodifiableCollection(chunks);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int sz = in.readInt();
        chunks = new ArrayList<Chunk>(sz);
        for (int i = 0; i < sz; i++) {
            final Chunk c = new Chunk();
            c.readFields(in);
            chunks.add(c);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(chunks.size());
        for (final Chunk c : chunks) {
            c.write(out);
        }
    }

    @Override
    public String toString() {
        return "Split [chunks=" + chunks + "]";
    }
}
