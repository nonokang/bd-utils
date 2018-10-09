package org.bd.es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> <br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年9月13日 上午12:05:19 |创建
 * 参考资料：https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/index.html
 */

public class Test {
	
	private TransportClient creatClient() throws UnknownHostException{
		// 设置集群名称
        Settings settings = Settings.builder()
        		.put("cluster.name", "ES-CLUSTER").build();
        // 创建client
        TransportClient client = new PreBuiltTransportClient(settings)
//        .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.111.103"), 9300))
        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		return client;
	}
	
	private IndexResponse createIndex(TransportClient client) throws IOException{
        IndexResponse response = client.prepareIndex("books", "book", "1")
                .setSource(jsonBuilder().startObject()
                        .field("book_name", "ElasticSearch")
                        .field("author", "张三")
                        .field("publish_time", new Date())
                       .endObject())
                .get();
        return response;
	}
	
	private void update(TransportClient client) throws IOException, InterruptedException, ExecutionException{
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("books");
		updateRequest.type("book");
		updateRequest.id("1");
		updateRequest.doc(jsonBuilder()
		        .startObject()
                .field("book_name", "ElasticSearch入口")
                .field("author", "李四")
                .field("publish_time", "208-09-09")
		        .endObject());
		client.update(updateRequest).get();
	}
	
	private GetResponse getByIndex(TransportClient client){
		GetResponse response = client.prepareGet("books", "book", "1").get();
		return response;
	}
	
	private DeleteResponse deleteByIndex(TransportClient client){
		DeleteResponse response = client.prepareDelete("books", "book", "1").get();
		return response;
	}
	
	private SearchResponse search(TransportClient client){
		SearchResponse response = client.prepareSearch("index1", "index2")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
		        .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .setFrom(0).setSize(60).setExplain(true)
		        .get();
//		SearchResponse response = client.prepareSearch().get();//获取所有的数据
		return response;
	}

	public static void main(String[] args) throws IOException {
		Test test = new Test();
		TransportClient client = test.creatClient();
		IndexResponse ir = test.createIndex(client);
		System.out.println(ir.toString());
        // 关闭client
        client.close();
	}

}
