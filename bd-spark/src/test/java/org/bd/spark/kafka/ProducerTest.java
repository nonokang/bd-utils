package org.bd.spark.kafka;

public class ProducerTest {
	
	public static void main(String[] args) {
        UserKafkaProducer producerThread = new UserKafkaProducer("topic1");
        producerThread.start();
    }
}
