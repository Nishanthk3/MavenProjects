package zookeeper.MonitorServers;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.UUID;

public class App2 {
    private static String appName = "App2";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String uuid = UUID.randomUUID().toString();
        RegisterApplication.startZookeeperMonitor(appName, uuid);
    }
}
