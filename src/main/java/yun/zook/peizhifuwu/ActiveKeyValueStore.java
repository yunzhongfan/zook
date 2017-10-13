package yun.zook.peizhifuwu;

import java.nio.charset.Charset;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ActiveKeyValueStore extends ConnectionWatcher {
	Logger log = Logger.getLogger(ActiveKeyValueStore.class);
	private static final Charset CHARSET=Charset.forName("UTF-8");
	
    public void write(String path,String value) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if(stat==null){
            zk.create(path, value.getBytes(CHARSET),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else{
           Stat stac =  zk.setData(path, value.getBytes(CHARSET),-1);
           log.info("设置数据成功,版本为："+stac.getAversion());
            
        }
    }
    public String read(String path,Watcher watch) throws KeeperException, InterruptedException{
        byte[] data = zk.getData(path, watch, null);
        return new String(data,CHARSET);
        
    }
    
}