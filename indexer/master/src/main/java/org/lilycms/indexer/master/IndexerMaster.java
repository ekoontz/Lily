package org.lilycms.indexer.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.lilycms.indexer.model.api.*;
import org.lilycms.util.zookeeper.LeaderElection;
import org.lilycms.util.zookeeper.LeaderElectionCallback;
import org.lilycms.util.zookeeper.LeaderElectionSetupException;
import org.lilycms.util.zookeeper.ZooKeeperItf;
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

    private IndexerModelListener listener = new MyListener();

    private int currentMaxConsumerId;

    private JobStatusWatcher jobStatusWatcher;

    private Thread jobStatusWatcherThread;

    private BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

    private Thread eventWorkerThread;

    private final Log log = LogFactory.getLog(getClass());

    public IndexerMaster(ZooKeeperItf zk , WriteableIndexerModel indexerModel, Configuration mapReduceConf,
            Configuration mapReduceJobConf) {
        this.zk = zk;
        this.indexerModel = indexerModel;
        this.mapReduceConf = mapReduceConf;
        this.mapReduceJobConf = mapReduceJobConf;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException {
        eventWorkerThread = new Thread(new EventWorker());
        eventWorkerThread.start();

        jobStatusWatcher = new JobStatusWatcher();

        LeaderElection leaderElection = new LeaderElection(zk, "Indexer Master", "/lily/indexer/masters",
                new MyLeaderElectionCallback());
    }

    @PreDestroy
    public void stop() {
        eventWorkerThread.interrupt();
        try {
            eventWorkerThread.join();
        } catch (InterruptedException e) {
            log.info("Interrupted while joining eventWorkerThread.");
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

            if (jobStatusWatcherThread != null) {
                jobStatusWatcherThread.interrupt();
                jobStatusWatcherThread = null;
                // We don't wait for it to finish, since otherwise we will block further dispatching
                // of ZK events.
            }
        }
    }

    private boolean needsConsumerIdAssigned(IndexDefinition index) {
        return index.getState() == IndexState.READY && index.getMessageConsumerId() == null;
    }

    private boolean needsFullBuildStart(IndexDefinition index) {
        return index.getState() == IndexState.REQUEST_BUILD && index.getActiveBuildJobInfo() == null;
    }

    /**
     * TODO This is temporary code to find out the currently highest assigned message consumer ID.
     * It should be adapated once a real system for (un)registering MQ consumers is in place.
     */
    private void initMaxConsumerId() {
        int currentMaxId = 10000;
        for (IndexDefinition index : indexerModel.getIndexes()) {
            if (index.getMessageConsumerId() != null) {
                int id = Integer.parseInt(index.getMessageConsumerId());
                if (id > currentMaxId) {
                    currentMaxId = id;
                }
            }
        }
        this.currentMaxConsumerId = currentMaxId;
    }


    private void assignConsumer(String indexName) {
        try {
            String lock = indexerModel.lockIndex(indexName);
            try {
                // Read current situation of record and assure it is still actual
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                if (needsConsumerIdAssigned(index)) {
                    String newConsumerId = String.valueOf(++currentMaxConsumerId);
                    index.setMessageConsumerId(newConsumerId);
                    indexerModel.updateIndex(index);
                    log.info("Assigned message consumer ID " + newConsumerId + " to index " + indexName);
                }
            } finally {
                indexerModel.unlockIndex(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to assign a message consumer to index " + indexName, t);
        }
    }

    private void startFullIndexBuild(String indexName) {
        try {
            String lock = indexerModel.lockIndex(indexName);
            try {
                // Read current situation of record and assure it is still actual
                IndexDefinition index = indexerModel.getMutableIndex(indexName);
                if (needsFullBuildStart(index)) {
                    String jobId = FullIndexBuilder.startRebuildJob(index, mapReduceJobConf);

                    ActiveBuildJobInfo jobInfo = new ActiveBuildJobInfo();
                    jobInfo.setSubmitTime(System.currentTimeMillis());
                    jobInfo.setJobId(jobId);
                    index.setActiveBuildJobInfo(jobInfo);

                    index.setState(IndexState.BUILDING);

                    indexerModel.updateIndex(index);

                    log.info("Started index build job for index " + indexName + ", job ID =  " + jobId);
                }
            } finally {
                indexerModel.unlockIndex(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to start index build job for index " + indexName, t);
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
                            if (needsConsumerIdAssigned(index)) {
                                assignConsumer(index.getName());
                            }

                            if (needsFullBuildStart(index)) {
                                startFullIndexBuild(index.getName());
                            }

                            if (index.getActiveBuildJobInfo() != null) {
                                jobStatusWatcher.assureWatching(index.getName(), index.getActiveBuildJobInfo().getJobId());
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

        private JobClient client;

        public JobStatusWatcher() throws IOException {
            client = new JobClient(JobTracker.getAddress(mapReduceConf), mapReduceConf);
        }

        public void reset() {
            runningJobs.clear();
        }

        public void run() {
            while (true) {
                try {
                    Thread.sleep(5000);

                    for (Map.Entry<String, String> jobEntry : runningJobs.entrySet()) {
                        if (Thread.interrupted()) {
                            return;
                        }

                        RunningJob job = client.getJob(jobEntry.getValue());

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
                String lock = indexerModel.lockIndex(indexName);
                try {
                    // Read current situation of record and assure it is still actual
                    IndexDefinition index = indexerModel.getMutableIndex(indexName);

                    ActiveBuildJobInfo activeJobInfo = index.getActiveBuildJobInfo();

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

                    BuildJobInfo jobInfo = new BuildJobInfo();
                    jobInfo.setJobState(jobState);
                    jobInfo.setSuccess(success);
                    jobInfo.setJobId(jobId);

                    if (activeJobInfo != null)
                        jobInfo.setSubmitTime(activeJobInfo.getSubmitTime());

                    index.setLastBuildJobInfo(jobInfo);
                    index.setActiveBuildJobInfo(null);

                    index.setState(success ? IndexState.READY : IndexState.BUILD_FAILED);

                    runningJobs.remove(indexName);
                    indexerModel.updateIndex(index);

                    log.info("Marked index build job as finished for index " + indexName + ", job ID =  " + jobId);

                } finally {
                    indexerModel.unlockIndex(lock);
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
