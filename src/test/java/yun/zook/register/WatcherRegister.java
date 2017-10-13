package yun.zook.register;

import java.io.IOException;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class WatcherRegister {
	private static final Logger LOG = Logger.getLogger(WatcherRegister.class);
	
	private static final String LOG_PREFIX_OF_MAIN = "【Main】";
	private static final String CHILDREN_PATH 	= "/zookeeper/ch";
		private  ZooKeeper  zk= null;
		private static final String ZK_PATH	= "/zookeeper";
		private CountDownLatch connectedSemaphore = new CountDownLatch(1);
		
		public  WatcherRegister(){
			
		}
		public  WatcherRegister(String   connectString,Watcher watcher){
			try {
				zk = new ZooKeeper(connectString, 3000, watcher);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void testWacherdisabled(String path) throws KeeperException, InterruptedException{
				WatcherExample we1 = new WatcherExample(); 
				System.out.println(zk.getData(path, we1, null));;
		}
		
		public void testexistsWatcher(String path) throws KeeperException, InterruptedException{
			WatcherExample we1 = new WatcherExample(); 
			System.out.println(zk.exists(path, we1));
		}
		
		public void testexistsWatcher(String path,Stack  stack) throws KeeperException, InterruptedException{
			WatcherExample we1 = new WatcherExample(); 
			
		}
		
		
		/**
		 * 创建ZK连接
		 * @param connectString	 ZK服务器地址列表
		 * @param sessionTimeout   Session超时时间
		 */
		public void createConnection( String connectString, int sessionTimeout ) {
			this.releaseConnection();
			try {
				WatcherExample we1 = new WatcherExample(); 
				zk = new ZooKeeper( connectString, sessionTimeout,we1 );
				LOG.info( LOG_PREFIX_OF_MAIN + "开始连接ZK服务器" );
				connectedSemaphore.await();
			} catch ( Exception e ) {}
		}

		/**
		 * 关闭ZK连接
		 */
		public void releaseConnection() {
		if ( zk!=null ) {
				try {
					this.zk.close();
				} catch ( InterruptedException e ) {}
			}
		}

		/**
		 *  创建节点
		 * @param path 节点path
		 * @param data 初始数据内容
		 * @return
		 */
		public boolean createPath( String path, String data ) {
			try {
				this.zk.exists( path, true );
				LOG.info( LOG_PREFIX_OF_MAIN + "节点创建成功, Path: "
						+ this.zk.create( path, //
								data.getBytes(), //
								                  Ids.OPEN_ACL_UNSAFE, //
								                  CreateMode.PERSISTENT )
						+ ", content: " + data );
			} catch ( Exception e ) {}
			return true;
		}

		/**
		 * 读取指定节点数据内容
		 * @param path 节点path
		 * @return
		 */
		public String readData( String path, boolean needWatch ) {
			try {
				return new String( this.zk.getData( path, needWatch, null ) );
			} catch ( Exception e ) {
				return "";
			}
		}

		/**
		 * 更新指定节点数据内容
		 * @param path 节点path
		 * @param data  数据内容
		 * @return
		 */
		public boolean writeData( String path, String data ) {
			try {
				LOG.info( LOG_PREFIX_OF_MAIN + "更新数据成功，path：" + path + ", stat: " +
			                                                this.zk.setData( path, data.getBytes(), -1 ) );
			} catch ( Exception e ) {}
			return false;
		}

		/**
		 * 删除指定节点
		 * @param path 节点path
		 */
		public void deleteNode( String path ) {
			try {
				this.zk.delete( path, -1 );
				LOG.info( LOG_PREFIX_OF_MAIN + "删除节点成功，path：" + path );
			} catch ( Exception e ) {
				//TODO
			}
		}
		
		/**
		 * 删除指定节点
		 * @param path 节点path
		 */
		public Stat exists( String path, boolean needWatch ) {
			try {
				return this.zk.exists( path, needWatch );
			} catch ( Exception e ) {return null;}
		}
		
		/**
		 * 获取子节点
		 * @param path 节点path
		 */
		private List<String> getChildren( String path, boolean needWatch ) {
			try {
				return this.zk.getChildren( path, needWatch );
			} catch ( Exception e ) {return null;}
		}
		
		public void deleteAllTestPath(){
			this.deleteNode( CHILDREN_PATH );
			this.deleteNode( ZK_PATH );
		}
		
		public static void main(String[] args) throws KeeperException, InterruptedException {
			WatcherExample we = new WatcherExample(); 
/*			WatcherRegister register = new WatcherRegister("127.0.0.1:2182",we);
*/			
			WatcherRegister re = new WatcherRegister();
			/*register.testWacherdisabled("/zookeeper");
			//Thread.sleep(30000);
			System.out.println("===========");
			register.testexistsWatcher("/zookeeper");*/
			
			
			
			re.createConnection("127.0.0.1:2182", 30000);
			/*Stat  Stat = re.exists("/zookeeper", true);
			if(Stat!=null){
				Stat.getCtime();
				System.out.println(re.readData("/zookeeper", true));
			}*/
			
			//re.createConnection( CONNECTION_STRING, SESSION_TIMEOUT );
			//清理节点
			re.deleteAllTestPath();
			if ( re.createPath( ZK_PATH, System.currentTimeMillis()+"" ) ) {
				Thread.sleep( 3000 );
				//读取数据
				re.readData( ZK_PATH, true );
				//读取子节点
				re.getChildren( ZK_PATH, true );
				
				//更新数据
				re.writeData( ZK_PATH, System.currentTimeMillis()+"" );
				Thread.sleep( 3000 );
				//创建子节点
				re.createPath( CHILDREN_PATH, System.currentTimeMillis()+"" );
			}
			Thread.sleep( 3000 );
			//清理节点
			re.deleteAllTestPath();
			Thread.sleep( 3000 );
			re.releaseConnection();
			
			
		}
		
		
}
