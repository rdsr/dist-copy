package dist_copy.util;

import java.io.IOException;
import java.io.InputStream;

public class BoundedInputStream extends InputStream {
    private final long sz;
    private final InputStream in;
    private long read;

    public BoundedInputStream(InputStream in, long sz) {
        this.in = in;
        this.sz = sz;
    }

    @Override
    public int read() throws IOException {
        if (read >= sz) {
            return -1;
        }
        read += 1l;
        return in.read();
    }
}
