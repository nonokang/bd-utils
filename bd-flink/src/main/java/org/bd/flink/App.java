package org.bd.flink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.http.HttpHost;
import org.bd.flink.idgenerate.IdGeneratorType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    	Properties properties = new Properties();
    	properties.setProperty("bootstrap.servers", "localhost:9092");
    	properties.setProperty("zookeeper.connect", "localhost:2181");
    	properties.setProperty("group.id", "test");
    	DataStream<XDREnrichedMessage> stream = env.addSource(new FlinkKafkaConsumer<>("topic2", new SimpleStringSchema(), properties))
    			.map(JsonToXDRRawMessage.builder().build()).process(new ProcessFunction<XDRRawMessage, XDREnrichedMessage>() {
					@Override
					public void processElement(XDRRawMessage input,
							ProcessFunction<XDRRawMessage, XDREnrichedMessage>.Context arg1,
							Collector<XDREnrichedMessage> ouput) throws Exception {
						XDREnrichedMessage.XDREnrichedMessageBuilder builder = XDREnrichedMessage.builder();
						builder.apn(input.getApn());
						builder.cell_id(input.getCell_id());
						builder.country(input.getCountry());
						builder.enterprise_name(input.getEnterprise_name());
						builder.timestamp_event(input.getTimestamp_event());
						System.out.println(builder.toString());
						ouput.collect(builder.build());
					}
				});
    	
        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));
    	
        ElasticsearchSink.Builder<XDREnrichedMessage> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                XDREnrichedMessageSinkFunction.<XDREnrichedMessage>builder()
                        .esIndex("flink_index_test01")
                        .idGeneratorType(IdGeneratorType.valueOf("MD5"))
                        .build());
        
        esSinkBuilder.setBulkFlushMaxActions(1);
        stream.addSink(esSinkBuilder.build());
        
    	env.execute();
    	
    }
}
