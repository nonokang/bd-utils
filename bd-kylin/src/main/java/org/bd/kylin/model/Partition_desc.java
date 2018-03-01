package org.bd.kylin.model;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 分区设置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:38:51 |创建
 */
public class Partition_desc {

	private String partition_date_column;
	private String partition_date_format = "yyyy-MM-dd";
	private String partition_type = "APPEND";
	
	public String getPartition_date_column() {
		return partition_date_column;
	}
	public void setPartition_date_column(String partition_date_column) {
		this.partition_date_column = partition_date_column;
	}
	public String getPartition_date_format() {
		return partition_date_format;
	}
	public void setPartition_date_format(String partition_date_format) {
		this.partition_date_format = partition_date_format;
	}
	public String getPartition_type() {
		return partition_type;
	}
	public void setPartition_type(String partition_type) {
		this.partition_type = partition_type;
	}
	
}
