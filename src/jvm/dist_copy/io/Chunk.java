package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Strings;

public class Chunk implements Writable {
    Path path;
    Long blockSize;
    String host, rack;
    Long[] offsets;

    public Chunk() {}

    public Chunk(Path path, Long blockSize, String host, String rack, Collection<Long> offsets) {
        this.path = path;
        this.blockSize = blockSize;
        this.host = host;
        this.rack = rack;
        this.offsets = offsets.toArray(new Long[0]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        blockSize = in.readLong();
        if (in.readBoolean()) {
            host = Text.readString(in);
        }
        if (in.readBoolean()) {
            rack = Text.readString(in);
        }
        final int sz = in.readInt();
        offsets = new Long[sz];
        for (int i = 0; i < sz; i++) {
            offsets[i] = in.readLong();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        out.writeLong(blockSize);
        if (Strings.isNullOrEmpty(host)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(false);
            Text.writeString(out, host);
        }
        if (Strings.isNullOrEmpty(rack)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, rack);
        }
        out.writeInt(offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            out.writeLong(offsets[i]);
        }
    }

    @Override
    public String toString() {
        return "Chunk [path=" + path + ", blockSize=" + blockSize + ", host=" + host + ", rack=" + rack + ", offsets="
                + Arrays.toString(offsets) + "]";
    }
}
