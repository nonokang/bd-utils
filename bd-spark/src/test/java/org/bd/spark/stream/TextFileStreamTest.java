package org.bd.spark.stream;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 实时获取文件流处理<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 下午4:24:21 |创建
 */
public class TextFileStreamTest {

	public static void TextFileStreaming() throws Exception{
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TextFileStreaming");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));//每5秒计算一批线程
		JavaDStream<String> lines = jssc.textFileStream("C:/Users/Administrator/Desktop/sparkFile/TextFileStream");
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}

	
	public static void main(String[] arg) throws Exception{
		System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
		TextFileStreaming();
	}
}
