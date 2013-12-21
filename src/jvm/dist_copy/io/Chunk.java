package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Strings;

public class Chunk implements Writable {
    private Path path;
    private String host, rack;
    private Collection<Block> blocks;

    public Chunk() {}

    public Chunk(Path path, String host, String rack, Collection<Block> blocks) {
        this.path = path;
        this.host = host;
        this.rack = rack;
        this.blocks = new ArrayList<>(blocks);
    }

    public Path getPath() {
        return path;
    }

    public String getHost() {
        return host;
    }

    public String getRack() {
        return rack;
    }

    public Collection<Block> getBlocks() {
        return Collections.unmodifiableCollection(blocks);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        if (in.readBoolean()) {
            host = Text.readString(in);
        }
        if (in.readBoolean()) {
            rack = Text.readString(in);
        }
        int sz = in.readInt();
        blocks = new ArrayList<>(sz);
        for (int i = 0; i < sz; i++) {
            final Block b = new Block();
            b.readFields(in);
            blocks.add(b);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        serializeOptionalString(out, host);
        serializeOptionalString(out, rack);
        out.writeInt(blocks.size());
        for (final Block b : blocks) {
            b.write(out);
        }
    }


    @Override
    public String toString() {
        return "Chunk [path=" + path + ", host=" + host + ", rack=" + rack + ", blocks=" + blocks + "]";
    }

    private static void serializeOptionalString(DataOutput out, String s) throws IOException {
        if (Strings.isNullOrEmpty(s)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, s);
        }
    }
}
