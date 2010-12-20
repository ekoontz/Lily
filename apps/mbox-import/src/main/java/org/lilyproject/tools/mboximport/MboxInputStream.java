/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.mboximport;

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
            while (!atFromLine && !eof) {
                System.err.println("Not yet at next message, skipping line: " + new String(buffer, 0, currentLineLength));
                readLine();
            }
        }

        if (eof)
            return false;

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
