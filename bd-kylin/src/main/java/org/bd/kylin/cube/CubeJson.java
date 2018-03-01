package org.bd.kylin.cube;

import java.util.ArrayList;
import java.util.List;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> cube脚本<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午12:11:56 |创建
 */
public class CubeJson {

	private String name;
	private String model_name;
	private String description;
	private List<Dimensions> dimensions;
	private List<Measures> measures;
	private Rowkey rowkey;
	private List<Aggregation_groups> aggregation_groups;
	private Integer partition_date_start = 0;
	private List<Notify_list> notify_list;
	private Hbase_mapping hbase_mapping;
	private String retention_range = "0";
	private List<String> status_need_notify;
	private List<Long> auto_merge_time_ranges;
	private Integer engine_type = 2;
	private Integer storage_type = 2;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getModel_name() {
		return model_name;
	}
	public void setModel_name(String model_name) {
		this.model_name = model_name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public List<Dimensions> getDimensions() {
		return dimensions;
	}
	public void setDimensions(List<Dimensions> dimensions) {
		this.dimensions = dimensions;
	}
	public List<Measures> getMeasures() {
		return measures;
	}
	public void setMeasures(List<Measures> measures) {
		this.measures = measures;
	}
	public Rowkey getRowkey() {
		return rowkey;
	}
	public void setRowkey(Rowkey rowkey) {
		this.rowkey = rowkey;
	}
	public List<Aggregation_groups> getAggregation_groups() {
		return aggregation_groups;
	}
	public void setAggregation_groups(List<Aggregation_groups> aggregation_groups) {
		this.aggregation_groups = aggregation_groups;
	}
	public Integer getPartition_date_start() {
		return partition_date_start;
	}
	public void setPartition_date_start(Integer partition_date_start) {
		this.partition_date_start = partition_date_start;
	}
	public List<Notify_list> getNotify_list() {
		return notify_list;
	}
	public void setNotify_list(List<Notify_list> notify_list) {
		this.notify_list = notify_list;
	}
	public Hbase_mapping getHbase_mapping() {
		return hbase_mapping;
	}
	public void setHbase_mapping(Hbase_mapping hbase_mapping) {
		this.hbase_mapping = hbase_mapping;
	}
	public String getRetention_range() {
		return retention_range;
	}
	public void setRetention_range(String retention_range) {
		this.retention_range = retention_range;
	}
	public List<String> getStatus_need_notify() {
		if(status_need_notify == null){
			status_need_notify = new ArrayList<String>();
			status_need_notify.add("ERROR");
			status_need_notify.add("DISCARDED");
			status_need_notify.add("SUCCEED");
		}
		return status_need_notify;
	}
	public void setStatus_need_notify(List<String> status_need_notify) {
		this.status_need_notify = status_need_notify;
	}
	public List<Long> getAuto_merge_time_ranges() {
		if(auto_merge_time_ranges == null){
			auto_merge_time_ranges = new ArrayList<Long>();
			auto_merge_time_ranges.add(604800000l);
			auto_merge_time_ranges.add(2419200000l);
		}
		return auto_merge_time_ranges;
	}
	public void setAuto_merge_time_ranges(List<Long> auto_merge_time_ranges) {
		this.auto_merge_time_ranges = auto_merge_time_ranges;
	}
	public Integer getEngine_type() {
		return engine_type;
	}
	public void setEngine_type(Integer engine_type) {
		this.engine_type = engine_type;
	}
	public Integer getStorage_type() {
		return storage_type;
	}
	public void setStorage_type(Integer storage_type) {
		this.storage_type = storage_type;
	}
	
}
