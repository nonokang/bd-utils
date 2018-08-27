package org.bd.kylin.model;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> cube模型脚本<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:30:24 |创建
 */
public class ModelJson {

	private String name;
	private String description;
	private String fact_table;
	private String capacity = "MEDIUM";
	private List<Lookups> lookups;
	private List<Dimensions> dimensions;
	private List<String> metrics;
	private String filter_condition;
	private Partition_desc partition_desc;
	
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFact_table() {
		return fact_table;
	}
	public void setFact_table(String fact_table) {
		this.fact_table = fact_table;
	}
	public String getCapacity() {
		return capacity;
	}
	public void setCapacity(String capacity) {
		this.capacity = capacity;
	}
	public List<Lookups> getLookups() {
		return lookups;
	}
	public void setLookups(List<Lookups> lookups) {
		this.lookups = lookups;
	}
	public List<Dimensions> getDimensions() {
		return dimensions;
	}
	public void setDimensions(List<Dimensions> dimensions) {
		this.dimensions = dimensions;
	}
	public List<String> getMetrics() {
		return metrics;
	}
	public void setMetrics(List<String> metrics) {
		this.metrics = metrics;
	}
	public String getFilter_condition() {
		return filter_condition;
	}
	public void setFilter_condition(String filter_condition) {
		this.filter_condition = filter_condition;
	}
	public Partition_desc getPartition_desc() {
		return partition_desc;
	}
	public void setPartition_desc(Partition_desc partition_desc) {
		this.partition_desc = partition_desc;
	}
	
}