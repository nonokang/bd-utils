package org.bd.flink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.http.HttpHost;
import org.bd.flink.idgenerate.IdGeneratorType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import com.sun.org.apache.bcel.internal.generic.NEW;

/**
 * Hello world!
 *
 */
public class App03 {
    public static void main( String[] args ) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    	Properties properties = new Properties();
    	properties.setProperty("bootstrap.servers", "localhost:9092");
    	properties.setProperty("zookeeper.connect", "localhost:2181");
    	properties.setProperty("group.id", "test");
    	
    	DataStream<String> input1 = env.addSource(new FlinkKafkaConsumer<>("topic3", new SimpleStringSchema(), properties));
    	
    	
    	
    	Pattern<String, ?> pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String input) throws Exception {
                    	JSONObject jo = JSONObject.fromObject(input);
                    	System.out.println("--first:"+jo);
                    	if(jo.containsKey("aaa") && jo.getString("aaa").equals("d316")){
                    		return true;
                    	}
                        return false;
                    }
                })
                .next("middle")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String input) throws Exception {
                    	JSONObject jo = JSONObject.fromObject(input);
                    	System.out.println("--second:"+jo);
                    	if(jo.containsKey("bbb") && jo.getString("bbb").equals("d318")){
                    		return true;
                    	}
                        return false;
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String input) throws Exception {
                    	JSONObject jo = JSONObject.fromObject(input);
                    	System.out.println("--third:"+jo);
                    	if(jo.containsKey("ccc") && jo.getString("ccc").equals("2")){
                    		return true;
                    	}
                        return false;
                    }
                })
                .followedBy("fourth")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String input) throws Exception {
                    	JSONObject jo = JSONObject.fromObject(input);
                    	System.out.println("--fourth:"+jo);
                    	if(jo.containsKey("ddd") && jo.getString("ddd").equals("d316-1")){
                    		return true;
                    	}
                        return false;
                    }
                });

    	PatternStream<String> patternStream = CEP.pattern(input1, pattern);

        patternStream.select(new PatternSelectFunction<String, String>() {

			@Override
			public String select(Map<String, List<String>> pattern) throws Exception {
				List<String> list = new ArrayList<String>();
	            List<String> first = pattern.get("first");
	            List<String> second = pattern.get("second");
	            List<String> third = pattern.get("third");
	            List<String> fourth = pattern.get("fourth");
	            
	            list.addAll(first);
	            list.addAll(second);
	            list.addAll(third);
	            list.addAll(fourth);
	            
	            System.out.println("=============================");
				return list.get(0);
			}
        	
		}).print();
        
        //input4.addSink(esSinkBuilder.build());
        
    	env.execute();
    	
    }
}
