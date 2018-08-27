package org.bd.kylin.request;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 创建cube任务请求参数对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午9:44:21 |创建
 */
public class JobBuildRequest {

    private long startTime;

    private long endTime;

    private String buildType;

//    private boolean force;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getBuildType() {
        return buildType;
    }

    public void setBuildType(String buildType) {
        this.buildType = buildType;
    }

    /*public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }*/

}
