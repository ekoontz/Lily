package org.lilyproject.util.zookeeper;

public class ZkLockException extends Exception {
    public ZkLockException(String message) {
        super(message);
    }

    public ZkLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
