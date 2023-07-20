package zookeeper.MonitorServers;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ZookeeperMonitor {
    private static String ApplicationNodes = "/appNodes";
    private static ZooKeeper zookeeper;

    public static void main(String args[]) throws InterruptedException, IOException, KeeperException {
        zookeeper = new ZooKeeper("localhost:2181", 15000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("*********************************************************");
                System.out.println("Event Triggered for node = " + watchedEvent.getPath());
                System.out.println("Event type = " + watchedEvent.getType());
                System.out.println("*********************************************************");
                try {
                    startWatch();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });

        // create application path
        if (zookeeper.exists(ApplicationNodes, false) == null) {
            zookeeper.create(ApplicationNodes, "data".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
        }

        // watch for child nodes (registered applications)
        startWatch();

        while (true) {

        }
    }

    private static void startWatch() throws InterruptedException, KeeperException {
        if (zookeeper != null) {
            List<String> children = zookeeper.getChildren(ApplicationNodes, true, null);
            System.out.println("List of children = " + children);
        }
    }
}
