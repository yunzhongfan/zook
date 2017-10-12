package yun.zook.register;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class WatcherExample implements Watcher{


	private static final Logger LOG =Logger.getLogger(WatcherExample.class);
	AtomicInteger seq = new AtomicInteger();
	private static final int SESSION_TIMEOUT = 10000;
	private static final String CONNECTION_STRING = "test.zookeeper.connection_string:2181," +
			                                                                           "test.zookeeper.connection_string2:2181," +
			                                                                           "test.zookeeper.connection_string3:2181";
	private static final String ZK_PATH 				= "/zookeeper";
	private static final String CHILDREN_PATH 	= "/zookeeper/ch";
	private static final String LOG_PREFIX_OF_MAIN = "【Main】";
	
	private ZooKeeper zk = null;
	
	private CountDownLatch connectedSemaphore = new CountDownLatch( 1 );

	
	
	
	/*public static void main( String[] args ) {

		PropertyConfigurator.configure("src/main/resources/log4j.properties");
		
		AllZooKeeperWatcher sample = new AllZooKeeperWatcher();
		sample.createConnection( CONNECTION_STRING, SESSION_TIMEOUT );
		//清理节点
		sample.deleteAllTestPath();
		if ( sample.createPath( ZK_PATH, System.currentTimeMillis()+"" ) ) {
			ThreadUtil.sleep( 3000 );
			//读取数据
			sample.readData( ZK_PATH, true );
			//读取子节点
			sample.getChildren( ZK_PATH, true );
			
			//更新数据
			sample.writeData( ZK_PATH, System.currentTimeMillis()+"" );
			ThreadUtil.sleep( 3000 );
			//创建子节点
			sample.createPath( CHILDREN_PATH, System.currentTimeMillis()+"" );
		}
		ThreadUtil.sleep( 3000 );
		//清理节点
		sample.deleteAllTestPath();
		ThreadUtil.sleep( 3000 );
		sample.releaseConnection();
	}*/


	/**
	 * 收到来自Server的Watcher通知后的处理。
	 */
	public void process( WatchedEvent event ) {

	try {
		Thread.sleep( 200 );
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		if ( event==null) {
			return;
		}
		// 连接状态
		KeeperState keeperState = event.getState();
		// 事件类型
		EventType eventType = event.getType();
		// 受影响的path
		String path = event.getPath();
		String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";

		LOG.info( logPrefix + "收到Watcher通知" );
		LOG.info( logPrefix + "连接状态:\t" + keeperState.toString() );
		LOG.info( logPrefix + "事件类型:\t" + eventType.toString() );

		if ( KeeperState.SyncConnected == keeperState ) {
			// 成功连接上ZK服务器
			if ( EventType.None == eventType ) {
				LOG.info( logPrefix + "成功连接上ZK服务器" );
				connectedSemaphore.countDown();
			} else if ( EventType.NodeCreated == eventType ) {
				LOG.info( logPrefix + "节点创建" );
				//this.exists( path, true );
			} else if ( EventType.NodeDataChanged == eventType ) {
				LOG.info( logPrefix + "节点数据更新" );
				//LOG.info( logPrefix + "数据内容: " + this.readData( ZK_PATH, true ) );
			} else if ( EventType.NodeChildrenChanged == eventType ) {
				LOG.info( logPrefix + "子节点变更" );
			//	LOG.info( logPrefix + "子节点列表：" + this.getChildren( ZK_PATH, true ) );
			} else if ( EventType.NodeDeleted == eventType ) {
				LOG.info( logPrefix + "节点 " + path + " 被删除" );
			}

		} else if ( KeeperState.Disconnected == keeperState ) {
			LOG.info( logPrefix + "与ZK服务器断开连接" );
		} else if ( KeeperState.AuthFailed == keeperState ) {
			LOG.info( logPrefix + "权限检查失败" );
		} else if ( KeeperState.Expired == keeperState ) {
			LOG.info( logPrefix + "会话失效" );
		}

		LOG.info( "--------------------------------------------" );

	}

}
