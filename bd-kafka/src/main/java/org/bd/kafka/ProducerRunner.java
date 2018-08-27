package org.bd.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 生产者线程<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月26日 下午3:29:05 |创建
 */
public class ProducerRunner implements Runnable {
	
    private ProducerClient producer;
    
    public void run() {  
        int i=0;    
        producer = new ProducerClient("topic1");
        while(true){
            try {
            	RecordMetadata rm = producer.sendMsg("message: " + i++);  
            	System.out.println(rm.toString());
                TimeUnit.SECONDS.sleep(2);    
            } catch (InterruptedException e) {    
                e.printStackTrace();    
            } catch (ExecutionException e) {
				e.printStackTrace();
			}
        } 
    }
    
    public static void main(String[] args) {
    	ProducerRunner item = new ProducerRunner();
    	Thread t = new Thread(item);
    	t.start();
    }
    
}
