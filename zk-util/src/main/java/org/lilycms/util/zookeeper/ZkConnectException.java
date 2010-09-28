package org.lilycms.util.zookeeper;

public class ZkConnectException extends Exception {
    public ZkConnectException(String message) {
        super(message);
    }

    public ZkConnectException(String message, Exception cause) {
        super(message, cause);
    }
}
