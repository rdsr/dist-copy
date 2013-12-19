package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Chunk implements Writable {
    Path path;
    int blockSize;
    String host, rack;
    Integer[] offsets;

    public Chunk() {}

    public Chunk(Path path, int blockSize, String host, String rack, Collection<Integer> offsets) {
        this.path = path;
        this.blockSize = blockSize;
        this.host = host;
        this.rack = rack;
        this.offsets = offsets.toArray(new Integer[0]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        blockSize = in.readInt();
        host = Text.readString(in);
        rack = Text.readString(in);

        final int sz = in.readInt();
        offsets = new Integer[sz];
        for (int i = 0; i < sz; i++) {
            offsets[i] = in.readInt();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        out.writeInt(blockSize);
        Text.writeString(out, host);
        Text.writeString(out, rack);

        out.writeInt(offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            out.writeInt(offsets[i]);
        }
    }

    @Override
    public String toString() {
        return "Chunk [path=" + path + ", blockSize=" + blockSize + ", host=" + host + ", rack=" + rack + ", offsets="
                + Arrays.toString(offsets) + "]";
    }
}
