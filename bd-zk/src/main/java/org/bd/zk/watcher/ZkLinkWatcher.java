package org.bd.zk.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> zookeeper连接同步观察<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月3日 上午10:54:59 |创建
 */
public class ZkLinkWatcher implements Watcher{

	@Override
	public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.Disconnected) {//断开连接
        	System.out.print("====断开连接====");
        } else if(event.getState() == KeeperState.SyncConnected) {//同步连接
        	System.out.print("====同步连接====");
        } else if(event.getState() == KeeperState.Expired) {//过期
        	System.out.print("====过期====");
        } else if(event.getState() == KeeperState.AuthFailed){//验证失败
        	System.out.print("====验证失败====");
        }
		System.out.println("Receive watched event :" + event);  
	}
	

}
