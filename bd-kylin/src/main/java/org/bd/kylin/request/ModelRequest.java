package org.bd.kylin.request;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 创建模型请求参数<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午9:49:32 |创建
 */
public class ModelRequest {

    private String project;
    private String modelDescData;
    /*private String uuid;
    private String modelName;
    private boolean successful;
    private String message;*/

    public ModelRequest() {}

    /*public ModelRequest(String modelName, String modelDescData) {
        this.modelName = modelName;
        this.modelDescData = modelDescData;
    }*/

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public String getModelDescData() {
		return modelDescData;
	}

	public void setModelDescData(String modelDescData) {
		this.modelDescData = modelDescData;
	}

	/*public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getModelName() {
		return modelName;
	}

	public void setModelName(String modelName) {
		this.modelName = modelName;
	}

	public boolean isSuccessful() {
		return successful;
	}

	public void setSuccessful(boolean successful) {
		this.successful = successful;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}*/

}
