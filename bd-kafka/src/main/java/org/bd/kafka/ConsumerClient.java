package org.bd.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.bd.kafka.utils.PropertiesUtil;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> kafka消费客户端<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月26日 上午11:44:33 |创建
 */
public class ConsumerClient {    
    
    private KafkaConsumer<String, String> consumer = null;
    private String topic;
        
    public ConsumerClient(String topic){  
        this.topic = topic;   
        init();
    }
    
    /**
     * <b>描述：</b> 初始化消费
     * @author wpk | 2017年11月26日 下午1:12:57 |创建
     * @return void
     */
    public void init(){
    	if(null == consumer){
            Properties properties = PropertiesUtil.getInstance().getProperties("consumer.properties");
            consumer = new KafkaConsumer<String, String>(properties); 
            consumer.subscribe(Arrays.asList(topic));
    	}
    }
    
    /**
     * <b>描述：</b> 获取消费者
     * @author wpk | 2017年11月26日 下午1:13:37 |创建
     * @return KafkaConsumer<String,String>
     */
    /*public KafkaConsumer<String, String> getConsumer(){
    	return consumer;
    }*/
    
    /**
     * <b>描述：</b> 获取分区信息
     * @author wpk | 2017年11月26日 下午1:14:20 |创建
     * @return List<PartitionInfo>
     */
    public List<PartitionInfo> getPartition(){
        List<PartitionInfo> list = consumer.partitionsFor(topic);
        for(PartitionInfo p : list){
        	System.out.println(p.toString());
        }
        return list;
    }
    
    /**
     * <b>描述：</b> 获取所有topic对应的分区信息
     * @author wpk | 2017年11月26日 下午1:14:37 |创建
     * @return Map<String,List<PartitionInfo>>
     */
    public Map<String,List<PartitionInfo>> getListTopics(){
        Map<String,List<PartitionInfo>> map = consumer.listTopics();
        return map;
    }
    
    /**
     * <b>描述：</b> 获取所有topic
     * @author wpk | 2017年11月26日 下午1:15:16 |创建
     * @return List<String>
     */
    public List<String> getTopics(){
    	List<String> topics = new ArrayList<String>();
    	Map<String,List<PartitionInfo>> map = getListTopics();
        for(String key : map.keySet()){
        	topics.add(key);
        }
        return topics;
    }
    
    /**
     * <b>描述：</b> 获取消费信息
     * @author wpk | 2017年11月26日 下午1:16:49 |创建
     * @return ConsumerRecords<String,String>
     */
    public ConsumerRecords<String, String> getRecords(){
        ConsumerRecords<String, String> records = consumer.poll(100);
    	return records;
    }
    
    /**
     * <b>描述：</b> 关闭消费
     * @author wpk | 2017年11月26日 下午1:22:03 |创建
     * @return void
     */
    public void close(){
    	if(null != consumer){
        	consumer.close();
    	}
    }
    
    /**
     * <b>描述：</b> 配置参数enable.auto.commit=false的情况下，需要同步偏移量
     * @author wpk | 2017年11月26日 下午2:56:46 |创建
     * @return void
     */
    public void commitSync(){
    	if(null != consumer){
        	consumer.commitAsync();
    	}
    }
    
    /**
     * <b>描述：</b> 唤醒消费者，用于关闭另一个线程中的消费者
     * @author wpk | 2017年11月26日 下午3:15:47 |创建
     * @return void
     */
    public void wakeup() {
    	if(null != consumer){
            consumer.wakeup();
    	}
    } 
         
}    