package zookeeper.AcquireDistributedLock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;

public class ZookeeperDistributedLock {
    public static String rootNode = "/locks";
    static Random random = new Random();
    public String appName;
    private static ZooKeeper zookeeper;

    public ZookeeperDistributedLock(String appName) throws IOException {
        zookeeper = new ZooKeeper("localhost:2181", 15000, watchedEvent -> {
        });
        this.appName = appName;
    }

    public void processTask(String appName, Consumer callBack, Runnable runnable) throws InterruptedException {
        while (true) {
            Thread.sleep(random.nextInt(10) * 1000);

            callBack.accept(runnable);
        }
    }

    public void processTaskByAcquiringLock(Runnable runnable) throws Exception {
        String uuid = UUID.randomUUID().toString();

        Watcher watcher = getWatcher(uuid, runnable);
        zookeeper.addWatch(rootNode, watcher, PERSISTENT_RECURSIVE);

        // create a lock
        zookeeper.create(rootNode + "/" + "lock-", uuid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        getLockOrWait(appName, uuid, rootNode, runnable);
    }

    public Watcher getWatcher(String uuid, Runnable runnable) {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        getLockOrWait(appName, uuid, rootNode, runnable);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        return watcher;
    }

    public void getLockOrWait(String appName, String uuid, String rootNode, Runnable runnable) throws InterruptedException, KeeperException {
        List<String> children = zookeeper.getChildren(rootNode, false);
        if (children == null || children.size() == 0) {
            return;
        }
        children.sort(String::compareTo);
        byte data[] = zookeeper.getData(rootNode + "/" + children.get(0), false, null);
        if (data != null && new String(data).equalsIgnoreCase(uuid)) {
            System.out.println("-----------------------------");
            System.out.println(appName + " acquired the lock");
            System.out.println("-----------------------------");
            runnable.run();
            Thread.sleep(1000);
            System.out.println("-----------------------------");
            System.out.println(appName + " is releasing the lock");
            System.out.println("-----------------------------");
            zookeeper.delete(rootNode + "/" + children.get(0), -1);
        } else {
            System.out.println(appName + " is waiting for a lock");
            zookeeper.getChildren(rootNode, true);
        }
    }
}
