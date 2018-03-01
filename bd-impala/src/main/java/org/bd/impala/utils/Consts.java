package org.bd.impala.utils;


/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 常量类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年8月23日 上午11:18:51 |创建
 */
public class Consts {
	
	/**impala 连接配置信息参数*/
	//impala驱动类
	public static final String impala_driver = "impala.driver";
	//impala url连接地址
	public static final String impala_url = "impala.url";
	//impala连接用户
	public static final String impala_user = "impala_user";
	//impala连接密码
	public static final String impala_password = "impala.password";
	//初始化连接池连接数
	public static final String initialsize = "initialsize";
	//连接池最大连接数
	public static final String maxactive = "maxactive";
	//最小生存时间
	public static final String minEvictableIdleTimeMillis = "minEvictableIdleTimeMillis";
	//开启强行回收功能
	public static final String removeAbandoned = "removeAbandoned";
	//连接废弃超过3小时未关闭，就会被强行回收
	public static final String removeAbandonedTimeout = "removeAbandonedTimeout";
	//30秒检测一次需要强行回收的连接
	public static final String timeBetweenEvictionRunsMillis = "timeBetweenEvictionRunsMillis";
	
	//属性配置文件名称
	public static final String impala_properties = "impala.properties";
	
}
