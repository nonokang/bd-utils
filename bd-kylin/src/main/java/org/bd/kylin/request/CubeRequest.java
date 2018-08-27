package org.bd.kylin.request;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 创建cube请求参数对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午9:48:09 |创建
 */
public class CubeRequest {

    private String project;
    private String cubeDescData;
    /*private String uuid;
    private String cubeName;
    private String streamingData;
    private String kafkaData;
    private boolean successful;
    private String message;
    private String streamingCube;*/

    public CubeRequest() {}

    /*public CubeRequest(String cubeName, String cubeDescData) {
        this.cubeName = cubeName;
        this.cubeDescData = cubeDescData;
    }*/

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public String getCubeDescData() {
		return cubeDescData;
	}

	public void setCubeDescData(String cubeDescData) {
		this.cubeDescData = cubeDescData;
	}

	/*public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getCubeName() {
		return cubeName;
	}

	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
	}

	public String getStreamingData() {
		return streamingData;
	}

	public void setStreamingData(String streamingData) {
		this.streamingData = streamingData;
	}

	public String getKafkaData() {
		return kafkaData;
	}

	public void setKafkaData(String kafkaData) {
		this.kafkaData = kafkaData;
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
	}

	public String getStreamingCube() {
		return streamingCube;
	}

	public void setStreamingCube(String streamingCube) {
		this.streamingCube = streamingCube;
	}*/

}
