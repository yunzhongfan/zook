package yun.zook.queue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
 
/**
 * Synchronizing
 * <p/>
 * Author By: sunddenly工作室
 * Created Date: 2014-11-13
 */
public class Synchronizing extends TestMainClient {
    int size;
    String name;
    public static final Logger logger = Logger.getLogger(Synchronizing.class);
 
    /**
     * 构造函数
     *
     * @param connectString 服务器连接
     * @param root 根目录
     * @param size 队列大小
     */
    Synchronizing(String connectString, String root, int size) {
        super(connectString);
        this.root = root;
        this.size = size;
 
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                logger.error(e);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
        try {
            name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            System.out.println("name="+name);
        } catch (UnknownHostException e) {
            logger.error(e);
        }
 
    }
 
    /**
     * 加入队列
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
 
    void addQueue() throws KeeperException, InterruptedException{
     Stat stat=   zk.exists(root + "/start",true);
    /* if(stat==null){
    	 zk.create(root + "/start", new byte[0], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
     }*/
        zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        synchronized (mutex) {
            List<String> list = zk.getChildren(root, false);
            if (list.size() < size) {
                mutex.wait();
            } else {
                zk.create(root + "/start", new byte[0], Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        }
    }
 
    @Override
    public void process(WatchedEvent event) {
    	if(event==null){
    		System.out.println("event为空");
    	}else{
    		System.out.println("event为不空"+event.toString());
    	}
    	if(event.getState()==Event.KeeperState.SyncConnected){
    		System.out.println("创建连接123");
    	}else{
    		 if(event.getPath().equals(root + "/start") && event.getType() == Event.EventType.NodeCreated){
    	            System.out.println("得到通知");
    	            super.process(event);
    	            doAction();
    	        }
    	}
      
    }
 
    /**
     * 执行其他任务
     */
    private void doAction(){
        System.out.println("同步队列已经得到同步，可以开始执行后面的任务了");
    }
 
    public static void main(String args[]) {
        //启动Server
        String connectString = "localhost:2181";
        int size = 2;
        Synchronizing b = new Synchronizing(connectString, "/synchronizing", size);
        try{
            b.addQueue();
        } catch (KeeperException e){
            logger.error(e);
        } catch (InterruptedException e){
            logger.error(e);
        }
    }
}