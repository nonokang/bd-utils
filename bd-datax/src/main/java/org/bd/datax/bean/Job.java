package org.bd.datax.bean;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> datax作业配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月20日 下午9:07:52 |创建
 */
public class Job {

	private Setting setting;
	private List<Content> content;
	
	public Setting getSetting() {
		return setting;
	}
	public void setSetting(Setting setting) {
		this.setting = setting;
	}
	public List<Content> getContent() {
		return content;
	}
	public void setContent(List<Content> content) {
		this.content = content;
	}
	
}
