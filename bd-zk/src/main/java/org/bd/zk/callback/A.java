package org.bd.zk.callback;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;


public class A {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ZkClient zk = new ZkClient("localhost:2181", 30000);
		zk.subscribeDataChanges("/wpk", new IZkDataListener(){
			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
				System.out.println("数据变更");
			}
			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				System.out.println("数据删除");
			}
			
		});
		Test t = new Test();
		t.setId(1);
		t.setName("张鹏");
		zk.writeData("/wpk", t);
		Test sd = zk.readData("/wpk");
		System.out.println(sd.getId()+"\t"+sd.getName());
	}
	

}
