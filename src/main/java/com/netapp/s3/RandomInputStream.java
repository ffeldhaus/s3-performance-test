package com.netapp.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

public class RandomInputStream extends InputStream {

    private final Random random = new Random();

    // Size of the stream
    private final long size;
    private long pos;

    /**
     * @param size target size of the stream [byte]
     */
    public RandomInputStream(long size) {
        this.size = size;
        this.pos = 0;
    }

    @Override
    public int read() throws IOException {
        if (pos == size) {
            return -1;
        }

        int nextByte = random.nextInt(255);

        pos++;

        return nextByte;
    }
}