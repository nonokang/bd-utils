package org.bd.kylin.response;

import java.util.List;
import java.util.Set;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> kylin项目信息<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 下午2:51:46 |创建
 */
public class ProjectInfo {

	protected String uuid;
    protected long last_modified;
    protected String version;
    private String name;
    private Set<String> tables;
    private String owner;
    private String status;
    private long create_time_utc;
    private String last_update_time;
    private String description;
    private List<Realizations> realizations;
    private List<String> models;
    private Set<String> ext_filters;

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public long getLast_modified() {
		return last_modified;
	}

	public void setLast_modified(long last_modified) {
		this.last_modified = last_modified;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<String> getTables() {
		return tables;
	}

	public void setTables(Set<String> tables) {
		this.tables = tables;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getCreate_time_utc() {
		return create_time_utc;
	}

	public void setCreate_time_utc(long create_time_utc) {
		this.create_time_utc = create_time_utc;
	}

	public String getLast_update_time() {
		return last_update_time;
	}

	public void setLast_update_time(String last_update_time) {
		this.last_update_time = last_update_time;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<Realizations> getRealizations() {
		return realizations;
	}

	public void setRealizations(List<Realizations> realizations) {
		this.realizations = realizations;
	}

	public List<String> getModels() {
		return models;
	}

	public void setModels(List<String> models) {
		this.models = models;
	}

	public Set<String> getExt_filters() {
		return ext_filters;
	}

	public void setExt_filters(Set<String> ext_filters) {
		this.ext_filters = ext_filters;
	}
    
}
