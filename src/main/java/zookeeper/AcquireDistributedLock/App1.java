package zookeeper.AcquireDistributedLock;

import static zookeeper.AcquireDistributedLock.ZookeeperDistributedLock.random;

public class App1 {
    private static String appName = "app1";
    public static void main(String args[]) throws Exception {
        ZookeeperDistributedLock distributedLock = new ZookeeperDistributedLock(appName);

        Runnable runnable = () -> {
            System.out.println("Processed From " + appName + "--  Addition -- " + (random.nextInt(10) + random.nextInt(10)));
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
