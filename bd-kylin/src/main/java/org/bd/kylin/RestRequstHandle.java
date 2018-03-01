package org.bd.kylin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bd.kylin.utils.SysVarsUtils;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> cube请求处理类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月26日 上午9:48:30 |创建
 */
public abstract class RestRequstHandle {
	
	public static String basePath = "";
	
	public static enum RequestMethod{
		GET,POST,PUT,PATCH,DELETE
	}
	
	/**
	 * <b>描述：</b> 获取路径
	 * @author wpk | 2017年9月26日 上午9:52:00 |创建
	 * @return String
	 */
	protected static String getPath(){
		if(StringUtils.isEmpty(basePath)){
			basePath = SysVarsUtils.getInstance().getVarByName("kylin.path");
		}
		return basePath;
	}
	
	/**
	 * <b>描述：</b> 请求rest接口
	 * @author wpk | 2017年9月26日 上午10:01:00 |创建
	 * @param para	请求参数
	 * @param method	请求方式
	 * @param json	脚本
	 * @return String
	 */
	protected static String request(String para, RequestMethod method, String json){
        StringBuilder out = new StringBuilder();
		String path = getPath() + "/" + para;
//		String path =  "http://10.10.10.2:7070/kylin/api/" + para;
//		String path =  "http://10.10.10.23:7070/kylin/api/" + para;
		HttpURLConnection conn = null;
		BufferedReader br = null;
		try {
			URL url = new URL(path);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(method.name());
			conn.setDoOutput(true);
			conn.setRequestProperty("Authorization", "Basic QURNSU46S1lMSU4=");
			conn.setRequestProperty("Content-Type","application/json;charset=UTF-8");
			
        	if(json != null){
        		byte[] outputInBytes = json.getBytes("UTF-8");
                OutputStream os = conn.getOutputStream();
                os.write(outputInBytes);
                os.close();
        	}

            int code = conn.getResponseCode();
            //调用web服务
            if (code == 200) {
                InputStream inStream1 = conn.getInputStream();
                br = new BufferedReader(new InputStreamReader(inStream1)); 
                String line;
                while ((line = br.readLine()) != null) {
                	out.append(line);
                }
            }else{
            	InputStream is = conn.getErrorStream();
            	String result = "";
            	if(null != is){
                	result = IOUtils.toString(is, StandardCharsets.UTF_8);
            	}
            	throw new RuntimeException(" error code is " + conn.getResponseCode() + "\n" + result + "\n" + conn.getResponseMessage());
            }
		} catch (Exception e) {
			throw new CubeException(String.format("kylin请求异常：%s", e));
		} finally{
            if(br != null){
                try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
            if(conn != null){
                conn.disconnect();
            }
		}
		return out.toString();
	}
	
	/**
	 * <b>描述：</b> 请求rest接口
	 * @author wpk | 2017年10月27日 下午3:15:42 |创建
	 * @param para
	 * @param method
	 * @return String
	 */
	protected static String request(String para, RequestMethod method){
		return request(para, method, null);
	}
}
