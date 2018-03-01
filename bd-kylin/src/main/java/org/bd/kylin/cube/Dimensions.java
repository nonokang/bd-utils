package org.bd.kylin.cube;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 维度<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午12:13:42 |创建
 */
public class Dimensions {

	private String table;
	private String column;
	private String name;
	private String derived = null;
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDerived() {
		return derived;
	}
	public void setDerived(String derived) {
		this.derived = derived;
	}
	
}
