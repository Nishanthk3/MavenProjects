package zookeeper.MonitorServers;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.UUID;

public class App1 {
    private static String appName = "App1";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String uuid = UUID.randomUUID().toString();
        RegisterApplication.startZookeeperMonitor(appName, uuid);
    }
}
