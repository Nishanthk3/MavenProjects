package zookeeper.MonitorServers;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class RegisterApplication {
    private static String ApplicationNodes = "/appNodes";
    private static final Logger log = LoggerFactory.getLogger(RegisterApplication.class);

    public static void startZookeeperMonitor(String appName, String uuid) throws IOException, InterruptedException, KeeperException {
        log.info("AppName :" + appName + " --  AppId  = " + uuid);
        ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 15000, null);
        String createResp = zookeeper.create(ApplicationNodes + "/" + uuid, uuid.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, null);
        log.info(createResp);
        while (true) {

        }
    }
}
