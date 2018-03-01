package org.bd.spark.enums;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 读\写资源格式<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月25日上午11:19:10 |创建
 */
public enum FormatType {
	
	/** txt文件类型*/
	TEXT("1", "text"),
	/** json文件类型*/
	JSON("2", "json"),
	/** csv文件类型*/
	CSV("3", "csv"),
	/** parquet文件类型*/
	PARQUET("4", "parquet"),
	/** 数据库类型*/
	JDBC("5", "jdbc");
	
	private final String state;
	private final String value;
	
	private FormatType(String state, String value){
		this.state = state;
		this.value = value;
	}
	
    public String getState() {
        return this.state;
    }

    public String getValue() {
        return this.value;
    }
	
}
