package dist_copy.io;

import java.io.IOException;
import java.io.InputStream;

import dist_copy.io.Split;

//TODO
public class SplitInputStream extends InputStream {
    Split split;
    int chunkIndex;
    int offset;
    
    @Override
    public int read() throws IOException {
        return 0;
    }
}
