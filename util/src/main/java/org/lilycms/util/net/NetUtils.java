package org.lilycms.util.net;

import java.io.IOException;
import java.net.ServerSocket;

public class NetUtils {
    public static int getFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Error finding a free port", e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing ServerSocket used to detect a free port.", e);
                }
            }
        }
    }
}
