package org.bd.kafka;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.ZooKeeper;
import org.bd.kafka.utils.PropertiesUtil;


/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> <br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月24日 上午11:49:47 |创建
 */
public class KafkaTopic {
	
	private ZkUtils zkUtils = null;
	private String host;
	
	public KafkaTopic(){
		if(null == zkUtils){
	        Properties properties = PropertiesUtil.getInstance().getProperties("zookeeper.properties");
	        this.host = properties.getProperty("zookeeper.connect");
			zkUtils = ZkUtils.apply(host, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		}
	}
	
	public static void main(String[] arg){
		KafkaTopic item = new KafkaTopic();
		item.listTopic();
//		item.deleteTopic("topic_wpk");
//		item.getTopics();
	}
	
	/**
	 * <b>描述：</b> 创建topic
	 * @author wpk | 2017年11月24日 下午4:01:04 |创建
	 * @param topic
	 * @return void
	 */
	public void createTopic(String topic){
		createTopic(1, 1, topic);
	}

	/**
	 * <b>描述：</b> 创建topic
	 * @author wpk | 2017年11月24日 下午4:01:43 |创建
	 * @param replications
	 * @param partitions
	 * @param topic
	 * @return void
	 */
	public void createTopic(Integer replications, Integer partitions, String topic){
        if(null == replications || 0 == replications){
        	throw new NullPointerException("[replications]参数为空");
        }
        if(null == partitions || 0 == partitions){
        	throw new NullPointerException("[partitions]参数为空");
        }
        if(null == topic || "".equals(topic)){
        	throw new NullPointerException("[topic]参数为空");
        }
		String[] arrys = new String[6];
        arrys[0] = "--replication-factor";
        arrys[1] = replications + "";
        arrys[2] = "--partitions";
        arrys[3] = partitions + "";
        arrys[4] = "--topic";
        arrys[5] = topic;
      
        TopicCommandOptions opts = new TopicCommandOptions(arrys);     
        TopicCommand.createTopic(zkUtils, opts); 
	}
	
	/**
	 * <b>描述：</b> 删除topic（要确保配置文件server.properties已经设置delete.topic.enable=true参数）
	 * @author wpk | 2017年11月24日 下午4:05:21 |创建
	 * @param topic
	 * @return void
	 */
	public void deleteTopic(String topic){
		String[] arrys = new String[2];
        arrys[0] = "--topic";
        arrys[1] = topic;

        TopicCommandOptions opts = new TopicCommandOptions(arrys);    
		TopicCommand.deleteTopic(zkUtils, opts);
	}
	
	/**
	 * <b>描述：</b> 获取topic
	 * @author wpk | 2017年11月24日 下午4:09:51 |创建
	 * @return void
	 */
	public List<String> listTopic(){
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream(1024*3);
		// cache stream
		PrintStream cacheStream = new PrintStream(byteStream);
		// 创建一个打印流out，此流已打开并准备接受输出数据。
		PrintStream oldStream = System.out;

		System.setOut(cacheStream);
		
        TopicCommandOptions opts = new TopicCommandOptions(new String[0]);    
		TopicCommand.listTopics(zkUtils, opts);

		// Restore old stream
		System.setOut(oldStream);
		
		String message = byteStream.toString();
		String[] ss = message.split("\r\n");
		List<String> ls = new ArrayList<String>();
		ls = Arrays.asList(ss);

		for(String topic : ls){
			System.out.println("topic:"+topic);
		}
		return ls;
	}
	
	/**
	 * <b>描述：</b> 获取topic
	 * @author wpk | 2017年11月25日 下午9:58:47 |创建
	 * @return List<String>
	 */
	public List<String> getTopics(){
		ZooKeeper zk = null;  
		List<String> list = null;
		try {
			zk = new ZooKeeper(host, 10000, null);  
	        list = zk.getChildren("/brokers/topics", false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for(String topic : list){
			System.out.println("topic:"+topic);
		}
		return list;
	}
	
}
