package org.lilycms.util.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.util.List;

/**
 * An interface for ZooKeeper.
 */
public interface ZooKeeperItf extends Closeable {
    
    void waitForConnection() throws InterruptedException;

    /**
     * Adds an additional watcher to be called as default watcher. This should mainly be used for
     * reacting to connection-related events.
     *
     * <p><b>This is a Lily-specific method.</b>
     */
    void addDefaultWatcher(Watcher watcher);

    /**
     * Removes a default watcher added via {@link #addDefaultWatcher}.
     *
     * <p><b>This is a Lily-specific method.</b>
     */
    void removeDefaultWatcher(Watcher watcher);

    /**
     * Perform the given operation, retrying in case of connection loss.
     *
     * <p>Note that in case of connection loss, you are never sure if the operation succeeded or not,
     * so it might be executed twice. Therefore:
     *
     * <ul>
     *   <li>in case of a delete operation, be prepared to deal with a NoNode exception
     *   <li>in case of a create operation, be prepared to deal with a NodeExists exception
     *   <li>in case of creation of a sequential node, two nodes might have been created. If they are ephemeral,
     *       you can use Stat.ephemeralOwner to find out the ones that belong to the current session. Otherwise,
     *       embed the necessary identification into the name or data.
     * </ul>
     *
     * <p>Do not call this method from within a ZooKeeper watcher callback, as it might block for a longer
     * time and hence block the delivery of other events, including the Disconnected event.
     *
     * <p><b>This is a Lily-specific method.</b>
     */
    <T> T retryOperation(ZooKeeperOperation<T> operation) throws InterruptedException, KeeperException;

    long getSessionId();

    byte[] getSessionPasswd();

    int getSessionTimeout();

    void addAuthInfo(String scheme, byte[] auth);

    void register(Watcher watcher);

    void close();

    String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException;

    void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx);

    void delete(String path, int version) throws InterruptedException, KeeperException;

    void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx);

    Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException;

    Stat exists(String path, boolean watch) throws KeeperException, InterruptedException;

    void exists(String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx);

    void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx);

    byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException;

    byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException;

    void getData(String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx);

    void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx);

    Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException;

    void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx);

    List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException;

    void getACL(String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx);

    Stat setACL(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException;

    void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx);

    List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException;

    List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException;

    void getChildren(String path, Watcher watcher, AsyncCallback.ChildrenCallback cb, Object ctx);

    void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx);

    List<String> getChildren(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException;

    List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException;

    void getChildren(String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx);

    void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx);

    void sync(String path, AsyncCallback.VoidCallback cb, Object ctx);

    ZooKeeper.States getState();
}
