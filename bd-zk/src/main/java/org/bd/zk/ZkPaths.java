package org.bd.zk;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> zookeeper树状路径(组合模式)<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月4日 下午3:40:19 |创建
 */
public class ZkPaths {

	private String name;
	private List<ZkPaths> children;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<ZkPaths> getChildren() {
		return children;
	}
	public void setChildren(List<ZkPaths> children) {
		this.children = children;
	}
	
}
