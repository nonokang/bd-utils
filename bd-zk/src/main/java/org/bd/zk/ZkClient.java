package org.bd.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.bd.zk.callback.ZkLinkAsyncCallback;
import org.bd.zk.utils.PropertiesUtil;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> <br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月3日 上午9:34:46 |创建
 */
public class ZkClient {
	
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private ZooKeeper zk = null;
	private String host = "";
	private int sessionTime = 10000;
	
	public ZkClient(){
		Properties p = PropertiesUtil.getInstance().getProperties("zookeeper.properties");
		host = p.getProperty("zookeeper.connect");
		sessionTime = Integer.parseInt(p.getProperty("sessionTimeout"));
		init();
	}
	
	/**
	 * <b>描述：</b> 初始化zookeeper
	 * @author wpk | 2017年12月5日 上午11:06:04 |创建
	 * @throws IOException
	 * @throws InterruptedException
	 * @return void
	 */
	public void init(){
		if(null == zk){
			try {
				zk = new ZooKeeper(host, sessionTime, new Watcher(){
				 	@Override
		            public void process(WatchedEvent event){
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
						countDownLatch.countDown();//线程执行完成
		            }
				});

//				dataChange(zk);
				countDownLatch.await(); //等待线程执行完成
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * <b>描述：</b> 获取zookeeper
	 * @author wpk | 2017年12月5日 上午11:07:41 |创建
	 * @return ZooKeeper
	 */
	public ZooKeeper getZk(){
		return zk;
	}
	
	/**
	 * <b>描述：</b> 创建同步znode
	 * @author wpk | 2017年12月4日 下午4:34:18 |创建
	 * @param path	创建路径
	 * @param data	创建内容
	 * @param ids	访问权限
	 * @param createMode	创建模式（临时节点，持久节点）
	 * @return void
	 */
	private void createSync(String path, String data, ArrayList<ACL> ids, CreateMode createMode){
		try {
			if(null == ids) ids = Ids.OPEN_ACL_UNSAFE;
			
			zk.create(path, data.getBytes(), ids, createMode);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * <b>描述：</b> 创建临时znode<br>
	 * 注意：如果创建相同的路径，不对其进行判断则将报NodeExistsException异常
	 * @author wpk | 2017年12月4日 下午4:36:03 |创建
	 * @param path
	 * @return void
	 */
	public void createEphemeral(String path){
		if(exits(path)) throw new ZkException(String.format("节点[%s]已存在!",path));
		createEphemeral(path, null);
	}
	
	/**
	 * <b>描述：</b> 创建临时znode <br>
	 * 注意：	<br>
	 * 参数isSequential为null或false时，创建的znode是唯一的，相同路径（znode）创建将报NodeExistsException异常	<br>
	 * 参数isSequential为true时，创建的znode也是唯一的，区别在于，如果创建多个相同的路径（znode），则会在该路径后边添加顺序号	<br>
	 * 例如重复创建znode路径为“wpk”,则出现以下结果：	<br>
	 * wpk	<br>
	 * wpk0000000034	<br>
	 * wpk0000000035	<br>
	 * ...	<br>
	 * @author wpk | 2017年12月4日 下午5:29:58 |创建
	 * @param path	创建znode节点
	 * @param isSequential	是否创建多个前缀相同的路径
	 * @return void
	 */
	public void createEphemeral(String path, Boolean isSequential){
		CreateMode cm = null;
		if(null == isSequential || !isSequential)
			cm = CreateMode.EPHEMERAL;
		else
			cm = CreateMode.EPHEMERAL_SEQUENTIAL;
		createSync(path, "", null, cm);
	}
	
	/**
	 * <b>描述：</b> 创建持久znode<br>
	 * 注意：如果创建相同的路径，不对其进行判断则将报NodeExistsException异常
	 * @author wpk | 2017年12月4日 下午4:36:17 |创建
	 * @param path
	 * @return void
	 */
	public void createPersistent(String path){
		if(exits(path)) throw new ZkException(String.format("节点[%s]已存在!",path));
		createPersistent(path, null);
	}
	
	/**
	 * <b>描述：</b> 创建持久znode <br>
	 * 注意：	<br>
	 * 参数isSequential为null或false时，创建的znode是唯一的，相同路径（znode）创建将报NodeExistsException异常	<br>
	 * 参数isSequential为true时，创建的znode也是唯一的，区别在于，如果创建多个相同的路径（znode），则会在该路径后边添加顺序号	<br>
	 * 例如重复创建znode路径为“wpk”,则出现以下结果：	<br>
	 * wpk	<br>
	 * wpk0000000034	<br>
	 * wpk0000000035	<br>
	 * ...	<br>
	 * @author wpk | 2017年12月4日 下午5:37:13 |创建
	 * @param path	创建znode节点
	 * @param isSequential	是否创建多个前缀相同的路径
	 * @return void
	 */
	public void createPersistent(String path, Boolean isSequential){
		CreateMode cm = null;
		if(null == isSequential || !isSequential)
			cm = CreateMode.PERSISTENT;
		else
			cm = CreateMode.PERSISTENT_SEQUENTIAL;
		createSync(path, "", null, cm);
	}

	/**
	 * <b>描述：</b> 创建异步znode
	 * @author wpk | 2017年12月4日 下午5:56:31 |创建
	 * @param path	创建路径
	 * @param data	创建内容
	 * @param ids	访问权限
	 * @param createMode	创建模式（临时节点，持久节点）
	 * @return void
	 */
	private void createAsync(String path, String data, ArrayList<ACL> ids, CreateMode createMode){
		if(null == ids) ids = Ids.OPEN_ACL_UNSAFE;
		
		zk.create(path, data.getBytes(), ids, createMode, new ZkLinkAsyncCallback(), "");
	}
	
	/**
	 * <b>描述：</b> 创建异步临时znode <br>
	 * 注意：如果创建相同的路径，不对其进行判断则将报NodeExistsException异常
	 * @author wpk | 2017年12月4日 下午5:58:16 |创建
	 * @param path
	 * @return void
	 */
	public void createAsyncEphemeral(String path){
		if(exits(path)) throw new ZkException(String.format("节点[%s]已存在!",path));
		createAsyncEphemeral(path, null);
	}
	
	/**
	 * <b>描述：</b> 创建异步临时znode <br>
	 * 注意：<br>
	 * 参数isSequential为null或false时，创建的znode是唯一的，相同路径（znode）创建将报NodeExistsException异常	<br>
	 * 参数isSequential为true时，创建的znode也是唯一的，区别在于，如果创建多个相同的路径（znode），则会在该路径后边添加顺序号	<br>
	 * 例如重复创建znode路径为“wpk”,则出现以下结果：<br>
	 * wpk	<br>
	 * wpk0000000034	<br>
	 * wpk0000000035	<br>
	 * ...	<br>
	 * @author wpk | 2017年12月4日 下午5:59:26 |创建
	 * @param path
	 * @param isSequential
	 * @return void
	 */
	public void createAsyncEphemeral(String path, Boolean isSequential){
		CreateMode cm = null;
		if(null == isSequential || !isSequential)
			cm = CreateMode.EPHEMERAL;
		else
			cm = CreateMode.EPHEMERAL_SEQUENTIAL;
		createAsync(path, "", null, cm);
	}
	
	/**
	 * <b>描述：</b> 创建异步持久znode<br>
	 * 注意：如果创建相同的路径，不对其进行判断则将报NodeExistsException异常
	 * @author wpk | 2017年12月4日 下午6:01:13 |创建
	 * @param path
	 * @return void
	 */
	public void createAsyncPersistent(String path){
		if(exits(path)) throw new ZkException(String.format("节点[%s]已存在!",path));
		createAsyncPersistent(path, null);
	}
	
	/**
	 * <b>描述：</b> 创建异步持久znode <br>
	 * 注意：	<br>
	 * 参数isSequential为null或false时，创建的znode是唯一的，相同路径（znode）创建将报NodeExistsException异常	<br>
	 * 参数isSequential为true时，创建的znode也是唯一的，区别在于，如果创建多个相同的路径（znode），则会在该路径后边添加顺序号	<br>
	 * 例如重复创建znode路径为“wpk”,则出现以下结果：
	 * wpk	<br>
	 * wpk0000000034	<br>
	 * wpk0000000035	<br>
	 * ...	<br>
	 * @author wpk | 2017年12月4日 下午6:01:44 |创建
	 * @param path
	 * @param isSequential
	 * @return void
	 */
	public void createAsyncPersistent(String path, Boolean isSequential){
		CreateMode cm = null;
		if(null == isSequential || !isSequential)
			cm = CreateMode.PERSISTENT;
		else
			cm = CreateMode.PERSISTENT_SEQUENTIAL;
		createAsync(path, "", null, cm);
	}
	
	/**
	 * <b>描述：</b> 获取指定路径下的子znode
	 * @author wpk | 2017年12月4日 下午4:36:41 |创建
	 * @param path
	 * @return List<String>
	 */
	public List<String> getChildren(String path){
		List<String> list = new ArrayList<String>();
		try {
			list = zk.getChildren(path, false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * <b>描述：</b> 获取所有znode(包括所有子znode)
	 * @author wpk | 2017年12月4日 下午4:37:14 |创建
	 * @return List<ZkPaths>
	 */
	public List<ZkPaths> getChildrens(){
		List<ZkPaths> paths = new ArrayList<ZkPaths>();
		recursionChildrens("/", paths);//递归获取子目录
		return paths;
	}
	
	private void recursionChildrens(String path, List<ZkPaths> paths){
		List<String> list = getChildren(path);
		for(String str : list){
			ZkPaths p = new ZkPaths();
			p.setName(str);
			List<ZkPaths> child = new ArrayList<ZkPaths>();
			StringBuffer newPath = new StringBuffer();
			newPath.append(path);
			if(!path.endsWith("/")) 
				newPath.append("/").append(str);
			else
				newPath.append(str);
			recursionChildrens(newPath.toString(), child);
			p.setChildren(child);
			paths.add(p);
		}
	}
	
	/**
	 * <b>描述：</b> 判断zookeeper是否还存活
	 * @author wpk | 2017年12月4日 下午8:41:56 |创建
	 * @return boolean
	 */
	public boolean isAlive(){
		return zk.getState().isAlive();
	}
	
	/**
	 * <b>描述：</b> 判断zookeeper是否断开连接
	 * @author wpk | 2017年12月4日 下午8:42:00 |创建
	 * @return boolean
	 */
	public boolean isConnected(){
		return zk.getState().isConnected();
	}
	
	/**
	 * <b>描述：</b> 获取znode信息
	 * @author wpk | 2017年12月4日 下午9:00:14 |创建
	 * @param path
	 * @return Stat
	 */
	public Stat getStat(String path){
		Stat stat = null;
		try {
			stat = zk.exists(path, false);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return stat;
	}
	
	/**
	 * <b>描述：</b> 判断是否存在znode
	 * @author wpk | 2017年12月4日 下午9:00:37 |创建
	 * @param path
	 * @return boolean
	 */
	public boolean exits(String path){
		if(null == getStat(path)){
			return false;
		}
		return true;
	}
	
	/**
	 * <b>描述：</b> 设置节点信息，如配置信息<br>
	 * 注意：设置信息时需要先获取节点的版本，当设置信息成功后，此znode将自动递增获取下个版本号
	 * @author wpk | 2017年12月4日 下午9:16:43 |创建
	 * @param path
	 * @param data
	 * @return Stat
	 */
	public Stat setData(String path, String data){
		Stat st = null;
		try {
			Stat stat = getStat(path);
			if(null == stat) throw new ZkException(String.format("不存在[%s]节点",path));
			st = zk.setData(path, data.getBytes(), stat.getVersion());
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return st;
	}
	
	/**
	 * <b>描述：</b> 同步获取znode信息
	 * @author wpk | 2017年12月4日 下午9:27:57 |创建
	 * @param path
	 * @return String
	 */
	public String getData(String path){
		String str = "";
		try {
			byte[] by = zk.getData(path, false, new Stat());
			str = new String(by);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return str;
	}
	
	/**
	 * <b>描述：</b> 同步删除znode
	 * @author wpk | 2017年12月4日 下午9:42:54 |创建
	 * @param path
	 * @return void
	 */
	public void delete(String path){
		try {
			Stat stat = getStat(path);
			if(null == stat) throw new ZkException(String.format("不存在[%s]节点",path));
			zk.delete(path, stat.getVersion());
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * <b>描述：</b> 通过堵塞方式进行持久的znode变更监控，可调用多次该方法来实现多个znode数据变更操作
	 * @author wpk | 2017年12月5日 上午11:26:46 |创建
	 * @param path
	 * @return void
	 */
	public void registerPersistentWatcher(String path){
		countDownLatch = new CountDownLatch(1);
		zk.register(new Watcher(){
		 	@Override
            public void process(WatchedEvent event){ 
		        if (event.getState() == KeeperState.Disconnected) {//断开连接
					countDownLatch.countDown();//线程执行完成
		        	System.out.print("===注册=断开连接====");
		        } else if(event.getState() == KeeperState.SyncConnected) {//同步连接
		        	System.out.print("==注册==同步连接====");
		        } else if(event.getState() == KeeperState.Expired) {//过期
					countDownLatch.countDown();//线程执行完成
		        	System.out.print("===注册=过期====");
		        } else if(event.getState() == KeeperState.AuthFailed){//验证失败
					countDownLatch.countDown();//线程执行完成
		        	System.out.print("==注册==验证失败====");
		        }
				System.out.println("注册Receive watched event :" + event); 
            }
		});
		try {
			dataChange(path);
			countDownLatch.await(); //等待线程执行完成
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * <b>描述：</b> 监控节点变更
	 * @author wpk | 2017年12月5日 上午11:27:26 |创建
	 * @param path
	 * @return void
	 */
	private void dataChange(String path){
        try {
			if (null != zk.exists(path, true)){
			    zk.getData(path, new Watcher(){
			        @Override
			        public void process(WatchedEvent event){
			            System.out.println("节点监听data change : " + event);
			            dataChange(path);
			        }
			    }, null);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
	
	public static void main(String[] arg){
		ZkClient zkClient = new ZkClient();
		
//		zkClient.createEphemeral("/wpk");
//		zkClient.createPersistent("/wpk");
		
//		zkClient.createAsyncPersistent("/wpk1");
//		zkClient.createAsyncEphemeral("/wpk1");
//		System.out.println("获取节点数据:"+zkClient.getData("/wpk"));
		
//		System.out.println("获取原来节点状态信息:"+zkClient.getStat("/wpk"));
//		System.out.println("获取节点设置后的状态信息:"+zkClient.setData("/wpk", "nihao123"));
		
//		zkClient.delete("/wpk0000000034");
		
		List<String> list = zkClient.getChildren("/");
		for (String str : list) {
			System.out.println(str);
		}
		
//		zkClient.registerPersistentWatcher("/wpk");
		
//		List<ZkPaths> paths = zkClient.getChildrens();
	}
	
}
