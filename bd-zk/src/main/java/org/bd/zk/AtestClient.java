package org.bd.zk;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AtestClient {

	public static void main(String[] arg) throws IOException, KeeperException, InterruptedException{
		ZkClient zkClient = new ZkClient();
		ZooKeeper zk = zkClient.getZk();
		System.out.println("获取节点数据:"+zk.getData("/wpk", false, new Stat()));
		System.out.println("获取节点设置后的状态信息:"+zk.setData("/wpk", "nihao123".getBytes(), -1));

		zkClient.registerPersistentWatcher("/wpk");
		
		/*ZooKeeper zk = new ZooKeeper("localhost:2181", 30000, new Watcher(){
	 	@Override
        public void process(WatchedEvent event){
			System.out.println("Receive watched event :" + event);  
        }
	});*/
	}
}
