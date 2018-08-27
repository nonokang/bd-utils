package org.bd.spark.enums;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 数据库类型<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月25日下午3:06:44 |创建
 */
public enum DbmsType {
	
	/** mysq数据库*/
	MYSQL{public String value(){return "mysql";}},
	/** oracle数据库*/
	ORACLE{public String value(){return "oracle";}},
	/** spark的jdbc连接服务*/
	SPARK{public String value(){return "spark";}};
	
	public abstract String value();
	
	/**
	 * <b>描述：获取枚举类的对象</b>
	 * @author wpk | 2017年7月25日下午6:23:23 |创建
	 * @param value
	 * @return
	 */
	public static DbmsType getByValue(String value){
		for(DbmsType ot : values()){
			if((ot.value()).equals(value))
				return ot;
		}
		return null;
	}
}
