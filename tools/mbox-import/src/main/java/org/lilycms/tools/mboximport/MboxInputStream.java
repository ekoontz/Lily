package org.lilycms.tools.mboximport;

import java.io.*;

/**
 * An input stream that allows to read one mime message at a time from an mbox file.
 *
 * <p>Each new message starts on a line starting with "From<space>", this line itself
 * is not part of the message.
 */
public class MboxInputStream extends InputStream {
    // TODO probably needs to support other line endings than \n
    private final static byte[] MSG_END_PATTERN = new byte[] {'\n', 'F', 'r', 'o', 'm', ' '};
    private final static byte[] MSG_START_PATTERN = new byte[] {'F', 'r', 'o', 'm', ' '};

    /** A circular buffer. */
    private int[] buffer = new int[MSG_END_PATTERN.length];

    /** Current 'zero' position in the buffer. */
    private int pos = 0;

    /** Length of the buffer. */
    private int length;

    private boolean endOfMsg;

    private InputStream delegate;

    public MboxInputStream(InputStream delegate) {
        this.delegate = delegate;
    }

    public boolean nextMessage() throws IOException {
        fillBuffer();

        if (!atStartOfMessage()) {
            return false;
        }

        endOfMsg = false;

        // search to the next newline, continue from there
        int next = read();
        while (next != -1 && next != '\n') {
            next = read();
        }

        return next != -1;
    }

    @Override
    public int read() throws IOException {
        if (endOfMsg)
            return -1;

        fillBuffer();
        if (atEndOfMessage()) {
            endOfMsg = true;
        }

        int result = buffer[pos];
        
        pos++;
        length--;
        if (pos == buffer.length)
            pos = 0;

        return result;
    }

    private void fillBuffer() throws IOException {
        while (length < buffer.length) {
            int writePos = pos + length < buffer.length ? pos + length : (pos + length) % buffer.length;
            buffer[writePos] = delegate.read(); // will fill the buffer with -1's when at end of input
            length++;
        }
    }

    private boolean atEndOfMessage() {
        for (int i = 0; i < MSG_END_PATTERN.length; i++) {
            int bufferPos = pos + i < buffer.length ? pos + i : (pos + i) % buffer.length;
            if (buffer[bufferPos] != MSG_END_PATTERN[i])
                return false;
        }
        return true;
    }

    private boolean atStartOfMessage() {
        for (int i = 0; i < MSG_START_PATTERN.length; i++) {
            int bufferPos = pos + i < buffer.length ? pos + i : (pos + i) % buffer.length;
            if (buffer[bufferPos] != MSG_START_PATTERN[i])
                return false;
        }
        return true;
    }
}
