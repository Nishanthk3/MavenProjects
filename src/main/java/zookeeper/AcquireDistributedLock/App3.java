package zookeeper.AcquireDistributedLock;

import static zookeeper.AcquireDistributedLock.ZookeeperDistributedLock.random;

public class App3 {
    private static String appName = "app3";

    public static void main(String args[]) throws Exception {
        ZookeeperDistributedLock distributedLock = new ZookeeperDistributedLock(appName);

        Runnable runnable = () -> {
            System.out.println("Processed From " + appName + " -- Multiplication -- " + (random.nextInt(10) * random.nextInt(10)));
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
