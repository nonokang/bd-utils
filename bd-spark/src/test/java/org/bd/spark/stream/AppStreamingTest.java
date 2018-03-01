package org.bd.spark.stream;


public class AppStreamingTest {

	public static void main(String[] args) {
		try {
			System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
			
			SocketStreamTest.socketStreaming();//socket流式处理
			
			TextFileStreamTest.TextFileStreaming();//监控指定目录下流式处理
			
//			KafkaStreamTest.kafkaStreaming();//kafka流式处理
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
