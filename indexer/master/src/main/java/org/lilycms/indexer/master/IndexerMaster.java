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

public class IndexerMaster {
    private ZooKeeperItf zk;

    private WriteableIndexerModel indexerModel;

    private Configuration mapReduceConf;

    private Configuration mapReduceJobConf;

    private Configuration hbaseConf;

    private String zkConnectString;

    private int zkSessionTimeout;

    private LeaderElection leaderElection;

    private IndexerModelListener listener = new MyListener();

    private int currentMaxConsumerId;

    private JobStatusWatcher jobStatusWatcher;

    private Thread jobStatusWatcherThread;

    private BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

    private Thread eventWorkerThread;

    private final Log log = LogFactory.getLog(getClass());

    public IndexerMaster(ZooKeeperItf zk , WriteableIndexerModel indexerModel, Configuration mapReduceConf,
            Configuration mapReduceJobConf, Configuration hbaseConf, String zkConnectString, int zkSessionTimeout) {
        this.zk = zk;
        this.indexerModel = indexerModel;
        this.mapReduceConf = mapReduceConf;
        this.mapReduceJobConf = mapReduceJobConf;
        this.hbaseConf = hbaseConf;
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        jobStatusWatcher = new JobStatusWatcher();

        leaderElection = new LeaderElection(zk, "Indexer Master", "/lily/indexer/masters",
                new MyLeaderElectionCallback());
    }

    @PreDestroy
    public void stop() {
        leaderElection.stop();
        
        if (eventWorkerThread != null) {
            eventWorkerThread.interrupt();
            try {
                eventWorkerThread.join();
            } catch (InterruptedException e) {
                log.info("Interrupted while joining eventWorkerThread.");
            }
        }

        if (jobStatusWatcherThread != null) {
            jobStatusWatcherThread.interrupt();
            try {
                jobStatusWatcherThread.join();
            } catch (InterruptedException e) {
                log.info("Interrupted while joining jobStatusWatcherThread.");
            }
        }
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        public void elected() {
            log.info("I am elected as the IndexerMaster.");

            // We do not need to worry about events already arriving, since this code is called
            // from a ZK callback.
            jobStatusWatcher.reset();

            eventQueue.clear(); // Just to be sure
            eventWorkerThread = new Thread(new EventWorker());
            eventWorkerThread.start();

            initMaxConsumerId();

            Collection<IndexDefinition> indexes = indexerModel.getIndexes(listener);

            // Rather than performing any work that might to be done for the indexes here,
            // we push out fake events. This is important because we do not want to do this
            // processing in this callback method (which blocks the ZK watcher and conflicts
            // with the ZkLock use)
            try {
                for (IndexDefinition index : indexes) {
                    eventQueue.put(new IndexerModelEvent(IndexerModelEventType.INDEX_UPDATED, index.getName()));
                }
            } catch (InterruptedException e) {
                log.info("Interrupted during initialisation following election as master.");
                return;
            }

            jobStatusWatcherThread = new Thread(jobStatusWatcher);
            jobStatusWatcherThread.start();
        }

        public void noLongerElected() {
            log.info("I am no longer the IndexerMaster.");

            indexerModel.unregisterListener(listener);

            eventQueue.clear();
            if (eventWorkerThread != null) {
                eventWorkerThread.interrupt();
                eventWorkerThread = null;
            }

            if (jobStatusWatcherThread != null) {
                jobStatusWatcherThread.interrupt();
                jobStatusWatcherThread = null;
                // We don't wait for it to finish, since otherwise we will block further dispatching
                // of ZK events.
            }
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

    /**
     * TODO This is temporary code to find out the currently highest assigned message consumer ID.
     * It should be adapated once a real system for (un)registering MQ consumers is in place.
     */
    private void initMaxConsumerId() {
        int currentMaxId = 10000;
        for (IndexDefinition index : indexerModel.getIndexes()) {
            if (index.getQueueSubscriptionId() != null) {
                int id = Integer.parseInt(index.getQueueSubscriptionId());
                if (id > currentMaxId) {
                    currentMaxId = id;
                }
            }
        }
        this.currentMaxConsumerId = currentMaxId;
    }


    private void assignSubscription(String indexName) {
        try {
            String lock = indexerModel.lockIndex(indexName);
            try {
                // Read current situation of record and assure it is still actual
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                if (needsSubscriptionIdAssigned(index)) {
                    String newConsumerId = String.valueOf(++currentMaxConsumerId);
                    index.setQueueSubscriptionId(newConsumerId);
                    indexerModel.updateIndexInternal(index);
                    log.info("Assigned queue subscription ID " + newConsumerId + " to index " + indexName);
                }
            } finally {
                indexerModel.unlockIndex(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to assign a queue subscription to index " + indexName, t);
        }
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
                    // TODO unregister message queue subscription once the subscription/listener distinction
                    //      is in place

                    // We can leave the subscription ID in the index definition FYI
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
                eventQueue.put(event);
            } catch (InterruptedException e) {
                log.info("IndexerMaster.IndexerModelListener interrupted.");
            }
        }
    }

    private class EventWorker implements Runnable {
        public void run() {
            while (true) {
                try {
                    if (Thread.interrupted()) {
                        return;
                    }

                    int queueSize = eventQueue.size();
                    if (queueSize >= 10) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }
                    
                    IndexerModelEvent event = eventQueue.take();

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
                    log.info("IndexerMaster.EventWorker interrupted.");
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

        public JobStatusWatcher() throws IOException {
        }

        public void reset() {
            runningJobs.clear();
        }

        public void run() {
            JobClient jobClient = null;
            while (true) {
                try {
                    Thread.sleep(5000);

                    for (Map.Entry<String, String> jobEntry : runningJobs.entrySet()) {
                        if (Thread.interrupted()) {
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
