package yun.zook.peizhifuwu;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;

public class ConnectionWatcher {
	
	protected  ZooKeeper zk = null;
	
	public void closeconnect() throws InterruptedException{
		if(zk!=null){
			zk.close();
		}
	}
	
	
	public  void connect(String host){
	
		try {
			this.closeconnect();
			ConfigWatcher cw = new ConfigWatcher();
			zk = new ZooKeeper(host, 3000, cw);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
