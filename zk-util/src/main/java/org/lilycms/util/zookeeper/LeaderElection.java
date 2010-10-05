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
 * Simple leader election system, not optimized for large numbers of potential leaders (could be improved
 * for herd effect, see ZK recipe).
 *
 * <p>It currently only reports if this client is the leader or not, it does not report who the leader is,
 * but that could be added.
 *
 * <p>It is intended for 'active leaders', which should give up their leader role as soon as we
 * are disconnected from the ZK cluster, rather than only when we get a session expiry event
 * (see http://markmail.org/message/o6whuii7wlf2a64c).
 *
 * <p>The leader state is reported via the {@link LeaderElectionCallback}, which is called from
 * within a different Thread than the ZooKeeper event thread.
 */
public class LeaderElection {
    private ZooKeeperItf zk;
    private String position;
    private String electionPath;
    private LeaderElectionCallback callback;
    private boolean elected = false;
    private ZkWatcher watcher = new ZkWatcher();
    private boolean stopped = false;
    private LeaderProvisioner leaderProvisioner = new LeaderProvisioner();

    enum LeaderState {
        I_AM_LEADER, I_AM_NOT_LEADER
    }

    private Log log = LogFactory.getLog(this.getClass());

    /**
     *
     * @param position a name for what position this leader election is about, used in informational messages.
     * @param electionPath path under which the ephemeral leader election nodes should be created. The path
     *                     will be created if it does not exist. The path should not end on a slash.
     */
    public LeaderElection(ZooKeeperItf zk, String position, String electionPath, LeaderElectionCallback callback)
            throws LeaderElectionSetupException, InterruptedException, KeeperException {
        this.zk = zk;
        this.position = position;
        this.electionPath = electionPath;
        this.callback = callback;
        proposeAsLeader();
        leaderProvisioner.start();
    }

    public void stop() throws InterruptedException {
        // Note that ZooKeeper does not have a way to remove watches (see ZOOKEEPER-422)
        stopped = true;
        leaderProvisioner.shutdown();
        if (leaderProvisioner.currentState == LeaderState.I_AM_LEADER) {
            try {
                callback.deactivateAsLeader();
            } catch (InterruptedException e) {
                throw e;
            } catch (Throwable t) {
                log.error("Error stopping the leader for " + position, t);
            }
        }
    }

    private void proposeAsLeader() throws LeaderElectionSetupException, InterruptedException, KeeperException {
        ZkUtil.createPath(zk, electionPath);

        try {
            // In case of connection loss, a node might have been created for us (we do not know it). Therefore,
            // retrying upon connection loss is important, so that we can continue with watching the leaders.
            // Later on, we do not look at the name of the node we created here, but at the owner.
            ZkUtil.retryOperationForever(new ZooKeeperOperation<String>() {
                public String execute() throws KeeperException, InterruptedException {
                    return zk.create(electionPath + "/n_", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                }
            });
        } catch (KeeperException e) {
            throw new LeaderElectionSetupException("Error creating leader election zookeeper node below " +
                    electionPath, e);
        }

        watchLeaders();
    }

    private synchronized void watchLeaders() {
        try {
            List<String> children = ZkUtil.retryOperationForever(new ZooKeeperOperation<List<String>>() {
                public List<String> execute() throws KeeperException, InterruptedException {
                    // Here the code could be improved: providing the watcher here can give the so-called
                    // "herd-effect", especially when there are many potential leaders.
                    return zk.getChildren(electionPath, watcher);
                }
            });
            // The child sequence numbers are fixed-with, prefixed with zeros, so we can sort them as strings
            Collections.sort(children);

            if (log.isDebugEnabled()) {
                log.debug("Leaders changed for the position of " + position + ", they are now:");
                for (String child : children) {
                    log.debug(child);
                }
            }

            // This list should never be empty, at least we are in it
            final String leader = children.get(0);

            // While we could compare the leader name with our own ephemeral node name, it is safer to compare
            // with the Stat.ephemeralOwner field. This is because the creation of the ephemeral node might have
            // failed with a ConnectionLoss exception, in which case we might have retried and hence have two
            // leader nodes allocated.
            Stat stat = ZkUtil.retryOperationForever(new ZooKeeperOperation<Stat>() {
                public Stat execute() throws KeeperException, InterruptedException {
                    return zk.exists(electionPath + "/" + leader, false);
                }
            });

            if (stat.getEphemeralOwner() == zk.getSessionId() && !elected) {
                elected = true;
                log.info("Elected as leader for the position of " + position);
                leaderProvisioner.setRequiredState(LeaderState.I_AM_LEADER);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
            if (stopped) {
                return;
            }

            if (event.getState().equals(Event.KeeperState.Disconnected) ||
                    event.getState().equals(Event.KeeperState.Expired)) {

                if (elected) {
                    elected = false;
                    log.info("No longer leader for the position of " + position);
                    leaderProvisioner.setRequiredState(LeaderState.I_AM_NOT_LEADER);
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

    /**
     * Activates or deactivates the leader. This is done in a separate thread, rather than in the
     * ZooKeeper event thread, in order not to block delivery of messages to other ZK watchers. A simple solution
     * to this would be to simply launch a thread in the LeaderElectionCallback methods. But then the handling
     * of the activateAsLeader() and deactivateAsLeader() could run in parallel, which we do not desire. An
     * improvement would be to put their processing in a queue. But when we have a lot of disconnected/connected
     * events in a short time frame, faster than they are processed, it would not make sense to process them one by
     * one, we are only interested in bringing the leader to latest requested state.
     *
     * Therefore, the solution used here just keeps a 'requiredState' variable and notifies a monitor when
     * it is changed.
     */
    private class LeaderProvisioner implements Runnable {
        private volatile LeaderState currentState = LeaderState.I_AM_NOT_LEADER;
        private volatile LeaderState requiredState = LeaderState.I_AM_NOT_LEADER;
        private final Object stateMonitor = new Object();
        private Thread thread;

        public synchronized void shutdown() throws InterruptedException {
            if (!thread.isAlive()) {
                return;
            }

            thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() {
            thread = new Thread(this, "LeaderProvisioner for " + position);
            thread.start();
        }

        public void run() {
            while (!Thread.interrupted()) {
                try {
                    if (currentState != requiredState) {
                        if (requiredState == LeaderState.I_AM_LEADER) {
                            callback.activateAsLeader();
                            currentState = LeaderState.I_AM_LEADER;
                        } else if (requiredState == LeaderState.I_AM_NOT_LEADER) {
                            callback.deactivateAsLeader();
                            currentState = LeaderState.I_AM_NOT_LEADER;
                        }
                    }

                    synchronized (stateMonitor) {
                        if (currentState == requiredState) {
                            stateMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // we stop working
                    return;
                } catch (Throwable t) {
                    log.error("Error in leader provisioner for " + position, t);
                }
            }
        }

        public void setRequiredState(LeaderState state) {
            synchronized (stateMonitor) {
                this.requiredState = state;
                stateMonitor.notifyAll();
            }
        }
    }
}
