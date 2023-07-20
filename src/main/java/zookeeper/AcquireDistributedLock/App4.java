package zookeeper.AcquireDistributedLock;

import static zookeeper.AcquireDistributedLock.ZookeeperDistributedLock.random;

public class App4 {
    private static String appName = "app4";

    public static void main(String args[]) throws Exception {
        ZookeeperDistributedLock distributedLock = new ZookeeperDistributedLock(appName);

        Runnable runnable = () -> {
            System.out.println("Processed From " + appName + " -- Division -- " + (random.nextInt(10) / random.nextInt(9)+1));
        };

        distributedLock.processTask(appName, a -> {
            try {
                distributedLock.processTaskByAcquiringLock((Runnable) a);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, runnable);
    }
}