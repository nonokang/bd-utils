package org.bd.spark.stream;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> kafka流处理<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:55:36 |创建
 */
public class KafkaStreamTest {
	
	public static void kafkaStreaming() throws Exception{
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("kafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		String topics = "topic1";
		Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
		String brokers = "192.168.32.115:9092";
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), 2L);
		//通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<Object,Object>> lines = KafkaUtils.createDirectStream(
        		jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets)
            );
        JavaPairDStream<String, Integer> counts = 
            lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
            .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
            .reduceByKey((x, y) -> x + y);  
		    counts.print();
		//可以打印所有信息，看下ConsumerRecord的结构
		  /*lines.foreachRDD(rdd -> {
		      rdd.foreach(x -> {
		        System.out.println(x);
		      });
		    });*/
		    jssc.start();
		    jssc.awaitTermination();
		    jssc.close();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
