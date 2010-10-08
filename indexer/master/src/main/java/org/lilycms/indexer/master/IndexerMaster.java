package org.lilycms.indexer.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.zookeeper.KeeperException;
import org.lilycms.indexer.model.api.*;
import org.lilycms.rowlog.api.RowLogConfigurationManager;
import org.lilycms.rowlog.api.RowLogSubscription;
import org.lilycms.util.zookeeper.*;

import static org.lilycms.indexer.model.api.IndexerModelEventType.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The indexer master is active on only one Lily node and is responsible for things such as launching
 * index batch build jobs, assigning or removing message queue subscriptions, and the like.
 */
public class IndexerMaster {
    private final ZooKeeperItf zk;

    private final WriteableIndexerModel indexerModel;

    private final Configuration mapReduceConf;

    private final Configuration mapReduceJobConf;

    private final Configuration hbaseConf;

    private final String zkConnectString;

    private final int zkSessionTimeout;

    private final RowLogConfigurationManager rowLogConfMgr;

    private LeaderElection leaderElection;

    private IndexerModelListener listener = new MyListener();

    private JobStatusWatcher jobStatusWatcher = new JobStatusWatcher();

    private EventWorker eventWorker = new EventWorker();

    private final Log log = LogFactory.getLog(getClass());

    public IndexerMaster(ZooKeeperItf zk , WriteableIndexerModel indexerModel, Configuration mapReduceConf,
            Configuration mapReduceJobConf, Configuration hbaseConf, String zkConnectString, int zkSessionTimeout,
            RowLogConfigurationManager rowLogConfMgr) {
        this.zk = zk;
        this.indexerModel = indexerModel;
        this.mapReduceConf = mapReduceConf;
        this.mapReduceJobConf = mapReduceJobConf;
        this.hbaseConf = hbaseConf;
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;
        this.rowLogConfMgr = rowLogConfMgr;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        leaderElection = new LeaderElection(zk, "Indexer Master", "/lily/indexer/masters",
                new MyLeaderElectionCallback());
    }

    @PreDestroy
    public void stop() {
        try {
            leaderElection.stop();
        } catch (InterruptedException e) {
            log.info("Interrupted while shutting down leader election.");
        }
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        public void activateAsLeader() throws Exception {
            log.info("Starting up as indexer master.");

            // Start these processes, but it is not until we have registered our model listener
            // that these will receive work.
            eventWorker.start();
            jobStatusWatcher.start();

            Collection<IndexDefinition> indexes = indexerModel.getIndexes(listener);

            // Rather than performing any work that might to be done for the indexes here,
            // we push out fake events. This way there's only one place where these actions
            // need to be performed.
            for (IndexDefinition index : indexes) {
                eventWorker.putEvent(new IndexerModelEvent(IndexerModelEventType.INDEX_UPDATED, index.getName()));
            }

            log.info("Startup as indexer master successful.");
        }

        public void deactivateAsLeader() throws Exception {
            log.info("Shutting down as indexer master.");

            indexerModel.unregisterListener(listener);

            // Argument false for shutdown: we do not interrupt the event worker thread: if there
            // was something running there that is blocked until the ZK connection comes back up
            // we want it to finish (e.g. a lock taken that should be released again)
            eventWorker.shutdown(false);
            jobStatusWatcher.shutdown(false);

            log.info("Shutdown as indexer master successful.");
        }
    }

    private boolean needsSubscriptionIdAssigned(IndexDefinition index) {
        return !index.getGeneralState().isDeleteState() &&
                index.getUpdateState() != IndexUpdateState.DO_NOT_SUBSCRIBE && index.getQueueSubscriptionId() == null;
    }

    private boolean needsFullBuildStart(IndexDefinition index) {
        return !index.getGeneralState().isDeleteState() &&
                index.getBatchBuildState() == IndexBatchBuildState.BUILD_REQUESTED && index.getActiveBatchBuildInfo() == null;
    }

    private void assignSubscription(String indexName) {
        try {
            String lock = indexerModel.lockIndex(indexName);
            try {
                // Read current situation of record and assure it is still actual
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                if (needsSubscriptionIdAssigned(index)) {
                    // We assume we are the only process which creates subscriptions which begin with the
                    // prefix "IndexUpdater:". This way we are sure there are no naming conflicts or conflicts
                    // due to concurrent operations (e.g. someone deleting this subscription right after we
                    // created it).
                    String subscriptionId = subscriptionId(index.getName());
                    rowLogConfMgr.addSubscription("MQ", subscriptionId, RowLogSubscription.Type.Netty, 3, 1);
                    index.setQueueSubscriptionId(subscriptionId);
                    indexerModel.updateIndexInternal(index);
                    log.info("Assigned queue subscription ID '" + subscriptionId + "' to index '" + indexName + "'");
                }
            } finally {
                indexerModel.unlockIndex(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to assign a queue subscription to index " + indexName, t);
        }
    }

    private String subscriptionId(String indexName) {
        return "IndexUpdater_" + indexName;
    }

    private void startFullIndexBuild(String indexName) {
        try {
            String lock = indexerModel.lockIndex(indexName);
            try {
                // Read current situation of record and assure it is still actual
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                if (needsFullBuildStart(index)) {
                    String jobId = FullIndexBuilder.startBatchBuildJob(index, mapReduceJobConf, hbaseConf,
                            zkConnectString, zkSessionTimeout);

                    ActiveBatchBuildInfo jobInfo = new ActiveBatchBuildInfo();
                    jobInfo.setSubmitTime(System.currentTimeMillis());
                    jobInfo.setJobId(jobId);
                    index.setActiveBatchBuildInfo(jobInfo);

                    index.setBatchBuildState(IndexBatchBuildState.BUILDING);

                    indexerModel.updateIndexInternal(index);

                    log.info("Started index build job for index " + indexName + ", job ID =  " + jobId);
                }
            } finally {
                indexerModel.unlockIndex(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to start index build job for index " + indexName, t);
        }
    }

    private void prepareDeleteIndex(String indexName) {
        // We do not have to take a lock on the index, since once in delete state the index cannot
        // be modified anymore by ordinary users.
        boolean canBeDeleted = false;
        try {
            // Read current situation of record and assure it is still actual
            IndexDefinition index = indexerModel.getMutableIndex(indexName);
            if (index.getGeneralState() == IndexGeneralState.DELETE_REQUESTED) {
                canBeDeleted = true;

                String queueSubscriptionId = index.getQueueSubscriptionId();
                if (queueSubscriptionId != null) {
                    rowLogConfMgr.removeSubscription("MQ", index.getQueueSubscriptionId());
                    // We leave the subscription ID in the index definition FYI
                }

                if (index.getActiveBatchBuildInfo() != null) {
                    JobClient jobClient = new JobClient(JobTracker.getAddress(mapReduceConf), mapReduceConf);
                    String jobId = index.getActiveBatchBuildInfo().getJobId();
                    RunningJob job = jobClient.getJob(jobId);
                    if (job != null) {
                        job.killJob();
                        log.info("Kill index build job for index " + indexName + ", job ID =  " + jobId);
                    }
                    // Just to be sure...
                    jobStatusWatcher.assureWatching(index.getName(), index.getActiveBatchBuildInfo().getJobId());
                    canBeDeleted = false;
                }

                if (!canBeDeleted) {
                    index.setGeneralState(IndexGeneralState.DELETING);
                    indexerModel.updateIndexInternal(index);
                }
            } else if (index.getGeneralState() == IndexGeneralState.DELETING) {
                // Check if the build job is already finished, if so, allow delete
                if (index.getActiveBatchBuildInfo() == null) {
                    canBeDeleted = true;
                }
            }
        } catch (Throwable t) {
            log.error("Error preparing deletion of index " + indexName, t);
        }

        if (canBeDeleted){
            deleteIndex(indexName);
        }
    }

    private void deleteIndex(String indexName) {
        boolean success = false;
        try {
            indexerModel.deleteIndex(indexName);
            success = true;
        } catch (Throwable t) {
            log.error("Failed to delete index.", t);
        }

        if (!success) {
            try {
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                index.setGeneralState(IndexGeneralState.DELETE_FAILED);
                indexerModel.updateIndexInternal(index);
            } catch (Throwable t) {
                log.error("Failed to set index state to " + IndexGeneralState.DELETE_FAILED, t);
            }
        }
    }

    private class MyListener implements IndexerModelListener {
        public void process(IndexerModelEvent event) {
            try {
                // Let the events be processed by another thread. Especially important since
                // we take ZkLock's in the event handlers (see ZkLock javadoc).
                eventWorker.putEvent(event);
            } catch (InterruptedException e) {
                log.info("IndexerMaster.IndexerModelListener interrupted.");
            }
        }
    }

    private class EventWorker implements Runnable {

        private BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

        private boolean stop;

        private Thread thread;

        public synchronized void shutdown(boolean interrupt) throws InterruptedException {
            stop = true;
            eventQueue.clear();

            if (!thread.isAlive()) {
                return;
            }

            if (interrupt)
                thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() throws InterruptedException {
            if (thread != null) {
                log.warn("EventWorker start was requested, but old thread was still there. Stopping it now.");
                thread.interrupt();
                thread.join();
            }
            eventQueue.clear();
            stop = false;
            thread = new Thread(this, "IndexerMasterEventWorker");
            thread.start();
        }

        public void putEvent(IndexerModelEvent event) throws InterruptedException {
            if (stop) {
                throw new RuntimeException("This EventWorker is stopped, no events should be added.");
            }
            eventQueue.put(event);
        }

        public void run() {
            long startedAt = System.currentTimeMillis();

            while (!stop && !Thread.interrupted()) {
                try {
                    IndexerModelEvent event = null;
                    while (!stop && event == null) {
                        event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                    }

                    if (stop || event == null || Thread.interrupted()) {
                        return;
                    }

                    // Warn if the queue is getting large, but do not do this just after we started, because
                    // on initial startup a fake update event is added for every defined index, which would lead
                    // to this message always being printed on startup when more than 10 indexes are defined.
                    int queueSize = eventQueue.size();
                    if (queueSize >= 10 && (System.currentTimeMillis() - startedAt > 5000)) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }

                    if (event.getType() == INDEX_ADDED || event.getType() == INDEX_UPDATED) {
                        IndexDefinition index = null;
                        try {
                            index = indexerModel.getIndex(event.getIndexName());
                        } catch (IndexNotFoundException e) {
                            // ignore, index has meanwhile been deleted, we will get another event for this
                        }

                        if (index != null) {
                            if (index.getGeneralState() == IndexGeneralState.DELETE_REQUESTED ||
                                    index.getGeneralState() == IndexGeneralState.DELETING) {
                                prepareDeleteIndex(index.getName());

                                // in case of delete, we do not need to handle any other cases
                                continue;
                            }

                            if (needsSubscriptionIdAssigned(index)) {
                                assignSubscription(index.getName());
                            }

                            if (needsFullBuildStart(index)) {
                                startFullIndexBuild(index.getName());
                            }

                            if (index.getActiveBatchBuildInfo() != null) {
                                jobStatusWatcher.assureWatching(index.getName(), index.getActiveBatchBuildInfo().getJobId());
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error processing indexer model event in IndexerMaster.", t);
                }
            }
        }
    }

    private class JobStatusWatcher implements Runnable {
        /**
         * Key = index name, value = job ID.
         */
        private Map<String, String> runningJobs = new ConcurrentHashMap<String, String>(10, 0.75f, 2);

        private boolean stop;

        private Thread thread;

        public JobStatusWatcher() {
        }

        public synchronized void shutdown(boolean interrupt) throws InterruptedException {
            stop = true;
            runningJobs.clear();

            if (!thread.isAlive()) {
                return;
            }

            if (interrupt)
                thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() throws InterruptedException {
            if (thread != null) {
                log.warn("JobStatusWatcher start was requested, but old thread was still there. Stopping it now.");
                thread.interrupt();
                thread.join();
            }
            runningJobs.clear();
            stop = false;
            thread = new Thread(this, "IndexerBatchJobWatcher");
            thread.start();
        }

        public void run() {
            JobClient jobClient = null;
            while (!stop && !Thread.interrupted()) {
                try {
                    Thread.sleep(1000);

                    for (Map.Entry<String, String> jobEntry : runningJobs.entrySet()) {
                        if (stop || Thread.interrupted()) {
                            return;
                        }

                        if (jobClient == null) {
                            // We only create the JobClient the first time we need it, to avoid that the
                            // repository fails to start up when there is no JobTracker running.
                            jobClient = new JobClient(JobTracker.getAddress(mapReduceConf), mapReduceConf);
                        }

                        RunningJob job = jobClient.getJob(jobEntry.getValue());

                        if (job == null) {
                            markJobComplete(jobEntry.getKey(), jobEntry.getValue(), false, "job unknown");
                        } else if (job.isComplete()) {
                            String jobState = jobStateToString(job.getJobState());
                            boolean success = job.isSuccessful();
                            markJobComplete(jobEntry.getKey(), jobEntry.getValue(), success, jobState);
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error in index job status watcher thread.", t);
                }
            }
        }

        public synchronized void assureWatching(String indexName, String jobName) {
            if (stop) {
                throw new RuntimeException("Job Status Watcher is stopped, should not be asked to monitor jobs anymore.");
            }
            runningJobs.put(indexName, jobName);
        }

        private void markJobComplete(String indexName, String jobId, boolean success, String jobState) {
            try {
                // Lock internal bypasses the index-in-delete-state check, which does not matter (and might cause
                // failure) in our case.
                String lock = indexerModel.lockIndexInternal(indexName, false);
                try {
                    // Read current situation of record and assure it is still actual
                    IndexDefinition index = indexerModel.getMutableIndex(indexName);

                    ActiveBatchBuildInfo activeJobInfo = index.getActiveBatchBuildInfo();

                    if (activeJobInfo == null) {
                        // This might happen if we got some older update event on the index right after we
                        // marked this job as finished.
                        log.error("Unexpected situation: index build job completed but index does not have an active" +
                                " build job. Index: " + index.getName() + ", job: " + jobId + ". Ignoring this event.");
                        runningJobs.remove(indexName);
                        return;
                    } else if (!activeJobInfo.getJobId().equals(jobId)) {
                        // I don't think this should ever occur: a new job will never start before we marked
                        // this one as finished, especially since we lock when creating/updating indexes.
                        log.error("Abnormal situation: index is associated with index build job " +
                                activeJobInfo.getJobId() + " but expected job " + jobId + ". Will mark job as" +
                                " done anyway.");
                    }

                    BatchBuildInfo jobInfo = new BatchBuildInfo();
                    jobInfo.setJobState(jobState);
                    jobInfo.setSuccess(success);
                    jobInfo.setJobId(jobId);

                    if (activeJobInfo != null)
                        jobInfo.setSubmitTime(activeJobInfo.getSubmitTime());

                    index.setLastBatchBuildInfo(jobInfo);
                    index.setActiveBatchBuildInfo(null);

                    index.setBatchBuildState(IndexBatchBuildState.INACTIVE);

                    runningJobs.remove(indexName);
                    indexerModel.updateIndexInternal(index);

                    log.info("Marked index build job as finished for index " + indexName + ", job ID =  " + jobId);

                } finally {
                    indexerModel.unlockIndex(lock, true);
                }
            } catch (Throwable t) {
                log.error("Error trying to mark index build job as finished for index " + indexName, t);
            }
        }

        private String jobStateToString(int jobState) {
            String result = "unknown";
            switch (jobState) {
                case JobStatus.FAILED:
                    result = "failed";
                    break;
                case JobStatus.KILLED:
                    result = "killed";
                    break;
                case JobStatus.PREP:
                    result = "prep";
                    break;
                case JobStatus.RUNNING:
                    result = "running";
                    break;
                case JobStatus.SUCCEEDED:
                    result = "succeeded";
                    break;
            }
            return result;
        }
    }
}
