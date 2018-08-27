package org.bd.kylin.model;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 查询表<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:33:51 |创建
 */
public class Lookups {

	private String table;
	private Join join;
	
	public class Join{
		private String type;
		private List<String> primary_key;
		private List<String> foreign_key;
		
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public List<String> getPrimary_key() {
			return primary_key;
		}
		public void setPrimary_key(List<String> primary_key) {
			this.primary_key = primary_key;
		}
		public List<String> getForeign_key() {
			return foreign_key;
		}
		public void setForeign_key(List<String> foreign_key) {
			this.foreign_key = foreign_key;
		}
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Join getJoin() {
		return join;
	}

	public void setJoin(Join join) {
		this.join = join;
	}
}
