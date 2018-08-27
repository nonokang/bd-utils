package org.bd.kylin.cube;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> hbase表映射<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午12:34:42 |创建
 */
public class Hbase_mapping {

	private List<Column_family> column_family;
	
	public class Column_family{
		private String name = "F1";
		private List<Columns> columns;
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public List<Columns> getColumns() {
			return columns;
		}
		public void setColumns(List<Columns> columns) {
			this.columns = columns;
		}
	}
	
	public class Columns{
		private String qualifier = "M";
		private List<String> measure_refs;
		
		public String getQualifier() {
			return qualifier;
		}
		public void setQualifier(String qualifier) {
			this.qualifier = qualifier;
		}
		public List<String> getMeasure_refs() {
			return measure_refs;
		}
		public void setMeasure_refs(List<String> measure_refs) {
			this.measure_refs = measure_refs;
		}
	}

	public List<Column_family> getColumn_family() {
		return column_family;
	}

	public void setColumn_family(List<Column_family> column_family) {
		this.column_family = column_family;
	}
	
}
