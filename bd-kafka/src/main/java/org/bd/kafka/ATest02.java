package org.bd.kafka;

import java.util.UUID;
import java.util.concurrent.ExecutionException; 

import net.sf.json.JSONArray;
import net.sf.json.JSONObject; 

public class ATest02 {

	public static void main(String[] arg){
		final ProducerClient pc = new ProducerClient("topic3");
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					JSONArray jArray = getInfor();
					for (int i = 0; i < jArray.size(); i++) {
						Thread.sleep(2000);
						JSONObject jo = (JSONObject)jArray.get(i);
						System.out.println(jo);
						pc.sendMsg(jo.toString());
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}
	
	public static JSONArray getInfor(){
		JSONArray ja = new JSONArray();
		JSONObject jo1 = new JSONObject();
		jo1.put("aaa", "d316");
		jo1.put("bbb", "d318");
		jo1.put("ccc", "0");
		jo1.put("ddd", "d316-0");

		JSONObject jo2 = new JSONObject();
		jo2.put("aaa", "d3161");
		jo2.put("bbb", "d3181");
		jo2.put("ccc", "1");
		jo2.put("ddd", "d316-2");

		JSONObject jo3 = new JSONObject();
		jo3.put("aaa", "d3162");
		jo3.put("bbb", "d3182");
		jo3.put("ccc", "2");
		jo3.put("ddd", "d316-3");

		JSONObject jo4 = new JSONObject();
		jo4.put("aaa", "d3163");
		jo4.put("bbb", "d3183");
		jo4.put("ccc", "3");
		jo4.put("ddd", "d316-1");

		ja.add(jo1);
		ja.add(jo2);
		ja.add(jo3);
		ja.add(jo4);
		return ja;
	}
}
