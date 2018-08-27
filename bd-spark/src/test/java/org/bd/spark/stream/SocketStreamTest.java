package org.bd.spark.stream;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> socket流处理<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:52:42 |创建
 */
public class SocketStreamTest {

	public static void socketStreaming() throws Exception{
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));//每5秒计算一批线程
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("172.16.3.132", 9999);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		// Print the first ten elements of each RDD generated in this DStream to the console
//		wordCounts.print();
		
		wordCounts.dstream().saveAsTextFiles("C:/Users/Administrator/Desktop/sparkFile/wordCount/", "socketStream");
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}

	
	public static void main(String[] arg){
		
	}
}
