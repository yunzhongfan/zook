package yun.zook.selectLeader;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
 
import java.net.InetAddress;
import java.net.UnknownHostException;
 
/**
 * LeaderElection
 * <p/>
 * Author By: sunddenly工作室
 * Created Date: 2014-11-13
 */
public class LeaderElection extends TestMainClient {
    public static final Logger logger = Logger.getLogger(LeaderElection.class);
 
    public LeaderElection(String connectString, String root) {
        super(connectString);
        this.root = root;
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                logger.error(e);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }
 
    void findLeader() throws InterruptedException, UnknownHostException, KeeperException {
        byte[] leader = null;
        try {
            leader = zk.getData(root + "/leader", true, null);
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                logger.error(e);
            } else {
                throw e;
            }
        }
        if (leader != null) {
            following();
        } else {
            String newLeader = null;
            byte[] localhost = InetAddress.getLocalHost().getAddress();
            try {
                newLeader = zk.create(root + "/leader", localhost, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    logger.error(e);
                } else {
                    throw e;
                }
            }
            if (newLeader != null) {
                leading();
            } else {
                mutex.wait();
            }
        }
    }
 
    @Override
    public void process(WatchedEvent event) {
        if (event.getPath().equals(root + "/leader") && event.getType() == Event.EventType.NodeCreated) {
            System.out.println("得到通知");
            super.process(event);
            following();
        }
    }
 
    void leading() {
        System.out.println("成为领导者");
    }
 
    void following() {
        System.out.println("成为组成员");
    }
 
    public static void main(String[] args) {
        String connectString = "localhost:2181";
 
        LeaderElection le = new LeaderElection(connectString, "/GroupMembers");
        try {
            le.findLeader();
        } catch (Exception e) {
            logger.error(e);
        }
    }
}