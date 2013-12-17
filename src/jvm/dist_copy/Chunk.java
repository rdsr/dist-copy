package dist_copy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Chunk implements Writable {
    Text file;
    IntWritable blockSz;
    ArrayWritable offsets;

    public Chunk() {}

    public Chunk(String file, int blockSz, int[] offsets) {
        this.file = new Text(file);
        this.blockSz = new IntWritable(blockSz);
        this.offsets = new ArrayWritable(IntWritable.class);

        final IntWritable[] iws = new IntWritable[offsets.length];
        for (int i = 0; i < iws.length; i++) {
            iws[i] = new IntWritable(offsets[i]);
        }
        this.offsets.set(iws);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file.readFields(in);;
        blockSz.readFields(in);
        offsets.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        file.write(out);
        blockSz.write(out);
        offsets.write(out);
    }
}
