package org.bd.kafka;

import java.util.UUID;
import java.util.concurrent.ExecutionException; 

import net.sf.json.JSONObject; 

public class ATest {

	public static void main(String[] arg){
		test02();
	}
	
	public static void test02(){
		final ProducerClient pc = new ProducerClient("rta_xdr");
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				int i = 1;
				while (true) {
					try {
						pc.sendMsg(i+"");
						System.out.println("==="+i);
						i++;
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}
	
	public static void test01(){
		final ProducerClient pc = new ProducerClient("rta_xdr");
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				int i = 1;
				StringBuffer sb = new StringBuffer();
				while (true) {
					try {
						int c = (int) (Math.random() * 100);
						if(c > 30){
							pc.sendMsg(getInfor(c));
							sb.append(c).append(",");
							System.out.println(getInfor(c));
							if (i == 3) {
								i = 1;
								System.out.println(sb.toString());
								sb.setLength(0);
							} else {
								i++;
							}
						}
						Thread.sleep(5000);
						//System.out.println(getInfor(i++));
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}
	
	public static String getInfor(int i){
		JSONObject jo = new JSONObject();
		jo.put("apn", "apn");
		jo.put("cell_id", i);
		jo.put("country", "CN");
		jo.put("map_errorcode", (int) (Math.random() * 100));
		jo.put("enterprise_name", UUID.randomUUID().toString());
		jo.put("timestamp_event", System.currentTimeMillis()+"");
		return jo.toString();
	}
}
