package org.bd.kylin.request;

import java.io.Serializable;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 查询语句请求参数<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月20日 下午9:54:14 |创建
 */
public class SQLRequest implements Serializable {
	
    protected static final long serialVersionUID = 1L;

    private String sql;
    private String project;
    private Integer offset = 0;
    private Integer limit = 0;
    private boolean acceptPartial = true;

    public SQLRequest() {
    }

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
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

	public boolean getAcceptPartial() {
		return acceptPartial;
	}

	public void setAcceptPartial(boolean acceptPartial) {
		this.acceptPartial = acceptPartial;
	}

}
