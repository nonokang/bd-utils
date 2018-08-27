package org.bd.kylin.response;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> cube响应对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月20日 下午5:44:20 |创建
 */
public class CubeResponse {

	private String uuid;
	private long last_modified;
	private String version;
    private String name;
    private String owner;
    private String descriptor;
    private int cost;
    private String status;
    private long create_time_utc;
    
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
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getDescriptor() {
		return descriptor;
	}
	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
	}
	public int getCost() {
		return cost;
	}
	public void setCost(int cost) {
		this.cost = cost;
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
    
}
