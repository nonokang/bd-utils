package org.bd.kylin.model;

import java.util.List;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 维度<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:33:38 |创建
 */
public class Dimensions {

	private String table;
	private List<String> columns;
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	
}
