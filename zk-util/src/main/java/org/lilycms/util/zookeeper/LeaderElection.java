package org.lilycms.util.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * Simple leader election system, not optimized for large numbers of potential leaders (see ZK recipe
 * for how it could be improved).
 *
 * <p>It currently only reports if this client is the leader or not, it does not report who the leader is,
 * but that could be added.
 *
 * <p>It is intended for 'active leaders', which should give up their leader role as soon as we
 * are disconnected from the ZK cluster, rather than only when we get a session expiry event
 * (see http://markmail.org/message/o6whuii7wlf2a64c).
 */
public class LeaderElection {
    private ZooKeeper zk;
    private String position;
    private String electionPath;
    private String electionNodeName;
    private LeaderElectionCallback callback;
    private boolean elected = false;
    private ZkWatcher watcher = new ZkWatcher();

    private Log log = LogFactory.getLog(this.getClass());

    /**
     *
     * @param position a name for what position this leader election is about, used in informational messages.
     * @param electionPath path under which the ephemeral leader election nodes should be created. The path
     *                     will be created if it does not exist. The path should not end on a slash.
     */
    public LeaderElection(ZooKeeper zk, String position, String electionPath, LeaderElectionCallback callback)
            throws LeaderElectionSetupException {
        this.zk = zk;
        this.position = position;
        this.electionPath = electionPath;
        this.callback = callback;
        proposeAsLeader();
    }

    private void proposeAsLeader() throws LeaderElectionSetupException {
        try {
            ZkUtil.createPath(zk, electionPath);
        } catch (ZkPathCreationException e) {
            throw new LeaderElectionSetupException("Error creating ZooKeeper path " + electionPath, e);
        }

        String path;
        try {
            path = zk.create(electionPath + "/n_", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException e) {
            throw new LeaderElectionSetupException("Error creating leader election zookeeper node below " +
                    electionPath, e);
        } catch (InterruptedException e) {
            throw new LeaderElectionSetupException("Error creating leader election zookeeper node below " +
                    electionPath, e);
        }

        electionNodeName = path.substring((electionPath + "/").length());

        watchLeaders();
    }

    private synchronized void watchLeaders() {
        try {
            List<String> children = ZkUtil.retryOperationForever(new ZooKeeperOperation<List<String>>() {
                public List<String> execute() throws KeeperException, InterruptedException {
                    return zk.getChildren(electionPath, watcher);
                }
            });
            Collections.sort(children);

            if (log.isDebugEnabled()) {
                log.debug("Leaders changed for the position of " + position + ", they are now: (me = " + electionNodeName + ")");
                for (String child : children) {
                    log.debug(child);
                }
            }

            // This list should never be empty, at least we are in it
            final String leader = children.get(0);

            // While we could compare the leader name with our own ephemeral node name, it seems
            // more defensive to use the Stat.ephemeralOwner field
            Stat stat = ZkUtil.retryOperationForever(new ZooKeeperOperation<Stat>() {
                public Stat execute() throws KeeperException, InterruptedException {
                    return zk.exists(electionPath + "/" + leader, false);
                }
            });

            if (stat.getEphemeralOwner() == zk.getSessionId() && !elected) {
                elected = true;
                callback.elected();

                log.info("I became the leader for the position of " + position);
            }

        } catch (InterruptedException e) {
            log.error("Error getting children of path " + electionPath, e);
        } catch (KeeperException e) {
            // If the exception happened on the zk.getChildren() call, then the watcher will not have been
            // set, and we not be notified of future changes anymore. Thus we will not know anymore if we could
            // be the leader, but if this is only a disconnected exception, we still could be without knowing!
            // However, above we keep retrying the operation forever, so we will only get here if the session
            // is really expired.
            log.error("Error getting children of path " + electionPath, e);
        }
    }

    public class ZkWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (event.getState().equals(Event.KeeperState.Disconnected) ||
                    event.getState().equals(Event.KeeperState.Expired)) {

                if (elected) {
                    elected = false;
                    log.info("I lost the leader role for the position of " + position);
                    callback.noLongerElected();
                }

                // Note that if we get a disconnected event here, our watcher is not unregistered, thus we will
                // get a connected event on this watcher once we are reconnected.

                // Since we are not owner of the ZooKeeper handle, we assume Expired states are handled
                // elsewhere in the application.

            } else if (event.getType() == EventType.None && event.getState() == KeeperState.SyncConnected) {
                // Upon reconnect, since our session was not expired, our ephemeral node will still
                // exist (it might even be the leader), therefore have a look at the leaders.
                watchLeaders();
            } else if (event.getType() == EventType.NodeChildrenChanged && event.getPath().equals(electionPath)) {
                watchLeaders();
            }
        }
    }

}
