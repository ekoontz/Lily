package org.lilycms.tools.mboximport;

import java.io.*;

/**
 * An input stream that allows to read one mime message at a time from an mbox file.
 *
 * <p>Each new message starts on a line starting with "From<space>", this line itself
 * is not part of the message.
 */
public class MboxInputStream extends InputStream {
    private InputStream delegate;
    private byte[] buffer;
    private int currentLineLength;
    private int currentLinePos;
    private boolean atFromLine;
    private boolean eof;

    public MboxInputStream(InputStream delegate, int maxLineLength) throws IOException {
        this.delegate = delegate;
        buffer = new byte[maxLineLength];
        readLine();
        currentLinePos = -1;
    }

    public boolean nextMessage() throws IOException {
        if (eof)
            return false;

        if (!atFromLine) {
            while (currentLineLength == 0) {
                readLine();
            }
            if (!atFromLine) {
                System.err.println("nextMessage is called, but we are not at a 'From ' line, current line is (length = " +
                        currentLineLength);
                System.err.println(new String(buffer, 0, currentLineLength));
                return false;
            }
        }

        readLine();

        return true;
    }

    @Override
    public int read() throws IOException {
        if (atFromLine || eof)
            return -1;

        currentLinePos++;

        if (currentLinePos >= currentLineLength) {
            readLine();
            currentLinePos = -1;
            return '\n';
        }

        return buffer[currentLinePos];
    }

    public int read(byte b[], int off, int len) throws IOException {
        if (atFromLine || eof)
            return -1;

        currentLinePos++;

        if (currentLinePos >= currentLineLength) {
            readLine();
            currentLinePos = -1;
            b[off] = '\n';
            return 1;
        }

        int amount = Math.min(currentLineLength - currentLinePos, len);

        System.arraycopy(buffer, currentLinePos, b, off, amount);

        currentLinePos += amount - 1;

        return amount;
    }

    private void readLine() throws IOException {
        int pos = 0;
        int next = delegate.read();

        if (next == -1) {
            eof = true;
            return;
        }

        while (next != -1 && next != '\n') {
            buffer[pos] = (byte)next;
            pos++;
            next = delegate.read();
        }
        currentLineLength = pos;

        atFromLine = currentLineLength >= 5 &&
                buffer[0] == 'F' && buffer[1] == 'r' && buffer[2] == 'o' && buffer[3] == 'm' && buffer[4] == ' ';
    }
}
