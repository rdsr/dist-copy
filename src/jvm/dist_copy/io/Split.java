package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

public class Split implements Writable {
    private Chunk[] chunks;

    public Split() {}

    public Split(Collection<Chunk> _chunks) {
        chunks = new Chunk[_chunks.size()];
        final int i = 0;
        for (final Chunk c : _chunks) {
            chunks[i] = c;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int sz = in.readInt();
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

    @Override
    public String toString() {
        return "Split [chunks=" + Arrays.toString(chunks) + "]";
    }
}
