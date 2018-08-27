package org.bd.kafka;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 消费者线程<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月26日 下午3:18:25 |创建
 */
public class ConsumerRunner implements Runnable {
	
    private AtomicBoolean closed = new AtomicBoolean(false);
    private ConsumerClient consumer;
    
    public void run() {
    	ConsumerClient item = new ConsumerClient("topic1");
        try {
            while (!closed.get()) {
                ConsumerRecords<String,String> records = item.getRecords();
                for (ConsumerRecord<String, String> record : records){
                    System.out.printf("topic = %s, partition = %s offset = %d, key = %s, value = %s\n",record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
        	item.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
    
    public static void main(String[] args) {    
    	ConsumerRunner item = new ConsumerRunner();// 使用kafka集群中创建好的主题 test   
    	Thread t = new Thread(item);
    	t.start();
    }   
}
