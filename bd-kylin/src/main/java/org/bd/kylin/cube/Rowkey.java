package org.bd.kylin.cube;

import java.util.List;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> rowkey配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午12:24:26 |创建
 */
public class Rowkey {
	
	private List<Rowkey_columns> rowkey_columns;
	
	public class Rowkey_columns{
		private String column;
		private String encoding = "dict";
		private Integer valueLength = 0;
		private String isShardBy = "false";
		
		public String getColumn() {
			return column;
		}
		public void setColumn(String column) {
			this.column = column;
		}
		public String getEncoding() {
			return encoding;
		}
		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}
		public Integer getValueLength() {
			return valueLength;
		}
		public void setValueLength(Integer valueLength) {
			this.valueLength = valueLength;
		}
		public String getIsShardBy() {
			return isShardBy;
		}
		public void setIsShardBy(String isShardBy) {
			this.isShardBy = isShardBy;
		}
	}

	public List<Rowkey_columns> getRowkey_columns() {
		return rowkey_columns;
	}

	public void setRowkey_columns(List<Rowkey_columns> rowkey_columns) {
		this.rowkey_columns = rowkey_columns;
	}

}
