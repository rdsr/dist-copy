package dist_copy.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A block denotes an hdfs block.
 * 
 * @author rdsr
 */
public class Block implements Writable {
    // Where in the hdfs file the block starts from.
    private long offset;
    // Amount of data currently in this block. Will always be <= block-size of file
    // i.e. The last block of a file may contain data less than the block-size
    private long len;
    // Set of hosts which contain this block
    private Set<String> hosts;
    // Set of racks which contain this block
    private Set<String> racks;

    public Block() {}

    public Block(long offset, long len, Set<String> hosts, Set<String> racks) {
        this.offset = offset;
        this.len = len;
        this.hosts = new HashSet<>(hosts);
        this.racks = new HashSet<>(racks);
    }

    public long getOffset() {
        return offset;
    }

    public long getLen() {
        return len;
    }

    public Set<String> getHosts() {
        return Collections.unmodifiableSet(hosts);
    }

    public Set<String> getRacks() {
        return Collections.unmodifiableSet(racks);
    }

    @Override
    public String toString() {
        return "Block [offset=" + offset + ", len=" + len + ", hosts=" + hosts + ", racks=" + racks + "]";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        offset = in.readLong();
        len = in.readLong();
        final int hsz = in.readInt();
        hosts = new HashSet<>(hsz);
        for (int i = 0; i < hsz; i++) {
            hosts.add(Text.readString(in));
        }
        final int rsz = in.readInt();
        racks = new HashSet<>(rsz);
        for (int i = 0; i < rsz; i++) {
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
