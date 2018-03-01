package org.bd.spark.kafka;

public class ConsumerTest {
	
	public static void main(String[] args) {
        UserKafkaConsumer consumerThread = new UserKafkaConsumer("topic1");
        consumerThread.start();
    }
}
