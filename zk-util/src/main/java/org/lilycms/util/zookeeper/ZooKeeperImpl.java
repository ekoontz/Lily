package org.lilycms.util.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * Default implementation of {@link ZooKeeperItf}.
 */
public class ZooKeeperImpl implements ZooKeeperItf {
    private ZooKeeper delegate;

    public ZooKeeperImpl() {

    }

    public ZooKeeperImpl(ZooKeeper delegate) {
        this.delegate = delegate;
    }

    public ZooKeeperImpl(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
        this.delegate = new ZooKeeper(connectString, sessionTimeout, watcher);
    }

    public void setDelegate(ZooKeeper delegate) {
        this.delegate = delegate;
    }

    public long getSessionId() {
        return delegate.getSessionId();
    }

    public byte[] getSessionPasswd() {
        return delegate.getSessionPasswd();
    }

    public int getSessionTimeout() {
        return delegate.getSessionTimeout();
    }

    public void addAuthInfo(String scheme, byte[] auth) {
        delegate.addAuthInfo(scheme, auth);
    }

    public void register(Watcher watcher) {
        delegate.register(watcher);
    }

    public void close() {
        try {
            delegate.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        return delegate.create(path, data, acl, createMode);
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        delegate.create(path, data, acl, createMode, cb, ctx);
    }

    public void delete(String path, int version) throws InterruptedException, KeeperException {
        delegate.delete(path, version);
    }

    public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
        delegate.delete(path, version, cb, ctx);
    }

    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return delegate.exists(path, watcher);
    }

    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return delegate.exists(path, watch);
    }

    public void exists(String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.exists(path, watcher, cb, ctx);
    }

    public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.exists(path, watch, cb, ctx);
    }

    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getData(path, watcher, stat);
    }

    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getData(path, watch, stat);
    }

    public void getData(String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
        delegate.getData(path, watcher, cb, ctx);
    }

    public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
        delegate.getData(path, watch, cb, ctx);
    }

    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return delegate.setData(path, data, version);
    }

    public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.setData(path, data, version, cb, ctx);
    }

    public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getACL(path, stat);
    }

    public void getACL(String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx) {
        delegate.getACL(path, stat, cb, ctx);
    }

    public Stat setACL(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        return delegate.setACL(path, acl, version);
    }

    public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
        delegate.setACL(path, acl, version, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watcher);
    }

    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watch);
    }

    public void getChildren(String path, Watcher watcher, AsyncCallback.ChildrenCallback cb, Object ctx) {
        delegate.getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
        delegate.getChildren(path, watch, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watcher, stat);
    }

    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return delegate.getChildren(path, watch, stat);
    }

    public void getChildren(String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx) {
        delegate.getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx) {
        delegate.getChildren(path, watch, cb, ctx);
    }

    public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
        delegate.sync(path, cb, ctx);
    }

    public ZooKeeper.States getState() {
        return delegate.getState();
    }

}
