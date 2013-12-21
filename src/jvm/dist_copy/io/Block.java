package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Block implements Writable {
    private long offset;
    private long len;
    private Collection<String> hosts;
    private Collection<String> racks;

    public Block() {}

    public Block(long offset, long len, Collection<String> hosts, Collection<String> racks) {
        this.offset = offset;
        this.len = len;
        this.hosts = new ArrayList<>(hosts);
        this.racks = new ArrayList<>(racks);
    }

    public long getOffset() {
        return offset;
    }

    public long getLen() {
        return len;
    }

    public Collection<String> getHosts() {
        return Collections.unmodifiableCollection(hosts);
    }

    public Collection<String> getRacks() {
        return Collections.unmodifiableCollection(racks);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        offset = in.readLong();
        len = in.readLong();
        hosts = new ArrayList<>(in.readInt());
        for (int i = 0; i < hosts.size(); i++) {
            hosts.add(Text.readString(in));
        }
        racks = new ArrayList<>(in.readInt());
        for (int i = 0; i < hosts.size(); i++) {
            racks.add(Text.readString(in));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeLong(len);
        out.writeInt(hosts.size());
        for (final String host : hosts) {
            Text.writeString(out, host);
        }
        out.writeInt(racks.size());
        for (final String rack : racks) {
            Text.writeString(out, rack);
        }
    }
}
