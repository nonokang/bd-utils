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

/**
 * Hello world!
 *
 */
public class App02 {
    public static void main( String[] args ) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    	Properties properties = new Properties();
    	properties.setProperty("bootstrap.servers", "localhost:9092");
    	properties.setProperty("zookeeper.connect", "localhost:2181");
    	properties.setProperty("group.id", "test");
    	
    	DataStream<String> input1 = env.addSource(new FlinkKafkaConsumer<>("topic3", new SimpleStringSchema(), properties));
    	
    	
    	
    	Pattern<String, ?> pattern = Pattern.<String>begin("start").where(
    	        new SimpleCondition<String>() {
    	        	private boolean str2Obj(String str){
    	        		JSONObject jo = JSONObject.fromObject(str);
    	        		if(!jo.containsKey("number")) return false;
    	        		int cellId = jo.getInt("number");
    	        		if(cellId <= 90) {
    	        			return true;
    	        		}
    	        		return false;
    	        	}
    	        	
    	            @Override
    	            public boolean filter(String event) {
    	                return str2Obj(event);
    	            }
    	        }
    	    ).or(new SimpleCondition<String>() {
	        	private boolean str2Obj(String str){
	        		JSONObject jo = JSONObject.fromObject(str);
	        		if(!jo.containsKey("number")) return false;
	        		int cellId = jo.getInt("number");
	        		if(cellId >= 40) {
	        			return true;
	        		}
	        		return false;
	        	}
	            @Override
	            public boolean filter(String event) {
	                return str2Obj(event);
	            }
			});

    	PatternStream<String> patternStream = CEP.pattern(input1, pattern);

    	DataStream<Tuple2<String, Integer>> input2 = patternStream.select(
    	    new PatternSelectFunction<String, Tuple2<String, Integer>>() {
    	    	
    	    	private Tuple2<String, Integer> createAlertFrom(Map<String, List<String>> pattern){
    	    		if(pattern.size()==1){
    	    			List<String> list = pattern.get("start");
    	    			if(list.size() == 1){
        	        		JSONObject jo = JSONObject.fromObject(list.get(0));
        	        		if(!jo.containsKey("number")) return new Tuple2<String, Integer>( "count", 0);
        	        		int number = jo.getInt("number");
        	    			return new Tuple2<String, Integer>( "alerNum", number);
    	    			}
    	    			return new Tuple2<String, Integer>( "alertNum", list.size());
    	    		}
    	    		return new Tuple2<String, Integer>( "alertNum", 0);
    	    	}
    	        @Override
    	        public Tuple2<String, Integer> select(Map<String, List<String>> pattern) throws Exception {
    	            return createAlertFrom(pattern);
    	        }
    	    }
    	);
    	
    	//第一个时间窗口
    	DataStream<Tuple2<String, Integer>> input3 = input2
    	.timeWindowAll(Time.seconds(5))
    	.reduce(new ReduceFunction<Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(
					Tuple2<String, Integer> v1,
					Tuple2<String, Integer> v2) throws Exception {
				return new Tuple2<>(System.currentTimeMillis()+"", v1.f1 + v2.f1);
			}
		});
    	
    	
    	//第二个时间窗口
    	//DataStream<String> input4 = 
    	input3
    	.timeWindowAll(Time.seconds(15))
    	.process(new ProcessAllWindowFunction<Tuple2<String,Integer>, String, TimeWindow>() {

			@Override
			public void process(
					ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>.Context context,
					Iterable<Tuple2<String, Integer>> input, Collector<String> ouput)
					throws Exception {
				Iterator it = input.iterator();
				int initNum = 0;
				boolean flag = true;
				JSONArray ja = new JSONArray();
				for (Tuple2<String, Integer> in: input) {
					if(in.f1 > initNum && flag){
						initNum = in.f1;
						flag = true;
						JSONObject jo = new JSONObject();
						jo.put("time", in.f0);
						jo.put("alertNum", in.f1);
						ja.add(jo);
					}else{
						flag = false;
					}
				}
				if(flag){
					ouput.collect("Window: " + context.window() + ja.toString());
				}else{
					ouput.collect("none");//不符合连续递增，没有数据
				}
			}
			
		}).print();
    	

        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
            httpHosts, new ElasticsearchSinkFunction<String>() {
                public IndexRequest createIndexRequest(String element) {
                    Map<String, String> json = new HashMap<>();
                    json.put("errorNum_alert_"+System.currentTimeMillis(), element);
                    return Requests.indexRequest()
                            .index("error_index")
                            .type("_doc")
                            .id(UUID.randomUUID().toString())
                            .source(json, XContentType.JSON);
                }
                
                @Override
                public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                    IndexRequest r = createIndexRequest(element);
                    if (r != null) {
                        //System.out.println(element);
                    	indexer.add(r);
                    }
                }
            }
        );
        
        
        esSinkBuilder.setBulkFlushMaxActions(1);
        
        //input4.addSink(esSinkBuilder.build());
        
    	env.execute();
    	
    }
}
