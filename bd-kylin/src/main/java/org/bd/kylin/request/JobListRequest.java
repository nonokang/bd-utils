package org.bd.kylin.request;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> job请求参数对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午10:16:39 |创建
 */
public class JobListRequest {

	/**
	 * 状态<br>
	 * job状态可以参考com.idata.bd.kylin.JobStatusEnum类<br>
	 * 默认获取所有状态
	 */
    private List<Integer> status;
    /**
     * cube名称
     */
    private String cubeName;
    /**
     * 项目名称
     */
    private String projectName;
    /**
     * 从第几行开始获取（用于分页下标）
     */
    private Integer offset;
    /**
     * 指定job数（用于分页）
     */
    private Integer limit;
    /**
     * 时间过滤<br>
     * job时间过滤可以参考com.idata.bd.kylin.JobTimeFilterEnum类<br>
     * 默认获取上个星期的作业
     */
    private Integer timeFilter;

    public JobListRequest() {}

    public List<Integer> getStatus() {
        return status;
    }

    public void setStatus(List<Integer> status) {
        this.status = status;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(Integer timeFilter) {
        this.timeFilter = timeFilter;
    }
}
