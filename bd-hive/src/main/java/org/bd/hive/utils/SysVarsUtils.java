package org.bd.hive.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;


/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 获取系统常量配置参数<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月31日 下午2:30:27 |创建
 */
public class SysVarsUtils {
	
	private Map<String,String> configCache = new HashMap<String, String>();
	
	private static SysVarsUtils instance;
	
	public static SysVarsUtils getInstance(){
		if(instance==null)instance = new SysVarsUtils();
		return instance;
	}
	
	/**清空缓存*/
	public void clearCache(){
		configCache.clear();
	}
	
	/**根据参数名取得参数值 */
	public String getVarByName(String name){
		if(StringUtils.isEmpty(name)){
			return null;
		}
		String config = configCache.get(name);
		if(config != null) {
			return config;
		}
		PropertiesUtil propertiesUtil = PropertiesUtil.getInstance();
		config = propertiesUtil.getPropertyValue(Consts.hive_properties, name);
		if(config != null){
			configCache.put(name, config);
		}
		return config;
	}
	
}
