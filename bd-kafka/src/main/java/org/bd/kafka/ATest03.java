package org.bd.kafka;

import java.util.UUID;
import java.util.concurrent.ExecutionException; 

import org.apache.kafka.clients.consumer.ConsumerRecords;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject; 

public class ATest03 {

	public static void main(String[] arg){
		final ConsumerClient cc = new ConsumerClient("rta_xdr");
		ConsumerRecords<String, String> cr = cc.getRecords();
		
		System.out.println(cr);
	}

}
