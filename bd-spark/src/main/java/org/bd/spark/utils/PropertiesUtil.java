package org.bd.spark.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 读取属性配置文件工具类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月20日上午11:40:06 |创建
 */
public class PropertiesUtil {
	private volatile static PropertiesUtil pu;// 创建对象pu
	private static Hashtable<String, Properties> register = new Hashtable<String, Properties>();
	private static Logger log = Logger.getLogger(PropertiesUtil.class);

	/** 默认构造函数*/
	private PropertiesUtil() {
		super();
	}

	/**
	 * <b>描述：取得PropertiesUtil的一个实例</b>
	 * @author wpk | 2017年7月25日下午6:12:48 |创建
	 * @return
	 */
	public static PropertiesUtil getInstance() {
		if (pu == null){
			synchronized (PropertiesUtil.class) {
				if(pu == null){
					pu = new PropertiesUtil();
				}
			}
		}
		return pu;
	}

	/**
	 * <b>描述：读取配置文件</b>
	 * @author wpk | 2017年7月25日下午6:24:41 |创建
	 * @param fileName
	 * @return
	 */
	private Properties getProperties(String fileName) {
		InputStream is = null;
		Properties p = null;
		try {
			p = (Properties) register.get(fileName);
			if (p == null) {
				try {
					is = new FileInputStream(fileName);
				} catch (Exception e) {
					if (fileName.startsWith("/"))
						is = PropertiesUtil.class.getResourceAsStream(fileName);
					else
						is = PropertiesUtil.class.getResourceAsStream("/" + fileName);
				}
				if (is == null) {
					log.info("未找到名称为" + fileName + "的资源！");
				}
				p = new Properties();
				p.load(is);
				register.put(fileName, p);
				is.close();
			}
		} catch (Exception e) {
			log.error("读取properties时异常", e);
		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					log.error("读取properties关闭流时异常", e);
				}
		}
		return p;
	}

	/**
	 * <b>描述：获取配置文件的指定值</b>
	 * @author wpk | 2017年7月23日上午1:27:29 |创建
	 * @param fileName
	 * @param strKey
	 * @return
	 */
	public String getPropertyValue(String fileName, String strKey) {
		Properties p = getProperties(fileName);
		try {
			return p.getProperty(strKey);
		} catch (Exception e) {
			log.error("读取properties时异常", e);
		}
		return null;
	}
	
	/**
	 * <b>描述：返回配置文件中指定数据库类型的基本连接参数</b>
	 * @author wpk | 2017年7月23日上午1:28:00 |创建
	 * @param fileName	配置文件名
	 * @param dbType	数据库类型（该值应与配置文件中的参数前缀一致）
	 * @return
	 * @throws Exception
	 */
	public Map<String,String> getValueByFile(String fileName,String dbType) throws Exception{
		Map<String,String> map = new HashMap<String,String>();
		Properties p = getProperties(fileName);
		if(p.containsKey(dbType+".jdbc.driver")){
			map.put("driver", p.getProperty(dbType+".jdbc.driver"));
		}else{
			throw new Exception(String.format("未找到[%s]参数", dbType+".jdbc.driver"));
		}
		if(p.containsKey(dbType+".jdbc.url")){
			map.put("url", p.getProperty(dbType+".jdbc.url"));
		}else{
			throw new Exception(String.format("未找到[%s]参数", dbType+".jdbc.url"));
		}
		if(p.containsKey(dbType+".jdbc.user")){
			map.put("user", p.getProperty(dbType+".jdbc.user"));
		}else{
			throw new Exception(String.format("未找到[%s]参数", dbType+".jdbc.user"));
		}
		if(p.containsKey(dbType+".jdbc.password")){
			map.put("password", p.getProperty(dbType+".jdbc.password"));
		}else{
			throw new Exception(String.format("未找到[%s]参数", dbType+".jdbc.password"));
		}
		return map;
	}

}
