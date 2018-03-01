package org.bd.hive.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 读取属性配置文件工具类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年8月23日 上午11:20:19 |创建
 */
public class PropertiesUtil {
	
	static PropertiesUtil pu;// 创建对象pu
	private static Hashtable<String, Properties> register = new Hashtable<String, Properties>();
	private static Logger log = Logger.getLogger(PropertiesUtil.class);

	private PropertiesUtil() {
		super();
	}

	/**
	 * 取得PropertiesUtil的一个实例
	 */
	public static PropertiesUtil getInstance() {
		if (pu == null)
			pu = new PropertiesUtil();
		return pu;
	}

	/**
	 * 读取配置文件
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
	
	public String getPropertyValue(String fileName, String strKey) {
		Properties p = getProperties(fileName);
		try {
			return p.getProperty(strKey);
		} catch (Exception e) {
			log.error("读取properties时异常", e);
		}
		return null;
	}

}
