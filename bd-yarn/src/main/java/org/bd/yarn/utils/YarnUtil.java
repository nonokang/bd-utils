package org.bd.yarn.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> yarn工具类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年9月1日 下午4:20:37 |创建
 */
public class YarnUtil {
	
	private YarnClient client;
	private Configuration conf;

	public YarnClient getClient(){
		if(client == null){
			Configuration conf = new Configuration();
			client = YarnClient.createYarnClient();
			client.init(conf);
			client.start();
		}
		return client;
	}
	
	public Configuration getConfig(){
		return conf;
	}
	
	public void close(){
		try {
			if(client == null){
				client.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
