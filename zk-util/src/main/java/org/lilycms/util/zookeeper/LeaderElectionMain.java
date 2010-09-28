package org.lilycms.util.zookeeper;

/**
 * Class to run the leader election by itself for test purposes.
 */
public class LeaderElectionMain implements Runnable {
    public static void main(String[] args) throws Exception {
        Thread t = new Thread(new LeaderElectionMain());
        t.setDaemon(false);
        t.start();

        while (!Thread.interrupted()){
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    public void run() {
        try {
            ZooKeeperItf zk = ZkUtil.connect("localhost:2181,localhost:21812", 5000);
            new LeaderElection(zk, "electiontest", "/lily/electiontest/leaders", new Callback());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class Callback implements LeaderElectionCallback {
        public void elected() {
            System.out.println("I am the leader.");
        }

        public void noLongerElected() {
            System.out.println("I am no longer the leader.");
        }
    }
}
