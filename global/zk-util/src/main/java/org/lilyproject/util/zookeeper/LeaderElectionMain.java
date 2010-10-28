/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.util.zookeeper;

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
        public void activateAsLeader() {
            System.out.println("I am the leader.");
        }

        public void deactivateAsLeader() {
            System.out.println("I am no longer the leader.");
        }
    }
}
