package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Split implements Writable {
    // Currently may contain just a single location (host/rack)
    private String[] locations;
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
        final int sz1 = in.readInt();
        locations = new String[sz1];
        for (int i = 0; i < sz1; i++) {
            locations[i] = Text.readString(in);
        }
        final int sz2 = in.readInt();
        chunks = new Chunk[sz2];
        for (int i = 0; i < sz2; i++) {
            chunks[i] = new Chunk();
            chunks[i].readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; i++) {
            Text.writeString(out, locations[i]);
        }
        out.writeInt(chunks.length);
        for (int i = 0; i < chunks.length; i++) {
            chunks[i].write(out);
        }
    }
}
