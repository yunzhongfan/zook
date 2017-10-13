package yun.zook.peizhifuwu;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ConfigWatcher implements Watcher{
	Logger log =Logger.getLogger(ConfigWatcher.class);
    private ActiveKeyValueStore store;
    
    public  ConfigWatcher(){
    	
    }

    public void process(WatchedEvent event) {

		try {
			if (event.getState() == KeeperState.SyncConnected) {
				log.info("连接成功！");
				if (event.getType() == EventType.NodeDataChanged) {
					dispalyConfig();
				} else if (EventType.NodeCreated == event.getType()) {
					log.info("节点创建");
					// this.exists( path, true );
				} else if (EventType.NodeDataChanged == event.getType()) {
					log.info("节点数据更新");
					// LOG.info( logPrefix + "数据内容: " + this.readData( ZK_PATH,
					// true ) );
				} else if (EventType.NodeChildrenChanged == event.getType()) {
					log.info("子节点变更");
					// LOG.info( logPrefix + "子节点列表：" + this.getChildren(
					// ZK_PATH, true ) );
				} else if (EventType.NodeDeleted == event.getType()) {
					log.info("节点 " + event.getPath() + " 被删除");
				}
			} else if (KeeperState.Disconnected == event.getState()) {
				log.info("与ZK服务器断开连接");
			} else if (KeeperState.AuthFailed == event.getState()) {
				log.info("权限检查失败");
			} else if (KeeperState.Expired == event.getState()) {
				log.info("会话失效");
			}
		} catch (InterruptedException e) {
			System.err.println("Interrupted. exiting. ");
			Thread.currentThread().interrupt();
		} catch (KeeperException e) {
			System.out.printf("KeeperException锛?s. Exiting.\n", e);
		}
	}
	
    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        store=new ActiveKeyValueStore();
        store.connect(hosts);
    }
    public void dispalyConfig() throws KeeperException, InterruptedException{
        String value=store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n",ConfigUpdater.PATH,value);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigWatcher configWatcher = new ConfigWatcher("127.0.0.1:2182");
        configWatcher.dispalyConfig();
        //stay alive until process is killed or Thread is interrupted
        Thread.sleep(Long.MAX_VALUE);
    }
}