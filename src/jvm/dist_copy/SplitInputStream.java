package dist_copy;

import java.io.IOException;
import java.io.InputStream;

import dist_copy.io.Split;

public class SplitInputStream extends InputStream {
    Split split;
    int chunkIndex;
    int offset;
    
    @Override
    public int read() throws IOException {
        return 0;
    }
}
