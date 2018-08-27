package org.bd.kylin.cube;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> <br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:15:47 |创建
 */
public class Aggregation_groups {

	private List<String> includes;
	private Select_rule select_rule;
	
	public class Select_rule{
		private List<String> mandatory_dims;
		private List<List<String>> hierarchy_dims;
		private List<List<String>> joint_dims;

		public List<String> getMandatory_dims() {
			return mandatory_dims;
		}
		public void setMandatory_dims(List<String> mandatory_dims) {
			this.mandatory_dims = mandatory_dims;
		}
		public List<List<String>> getHierarchy_dims() {
			return hierarchy_dims;
		}
		public void setHierarchy_dims(List<List<String>> hierarchy_dims) {
			this.hierarchy_dims = hierarchy_dims;
		}
		public List<List<String>> getJoint_dims() {
			return joint_dims;
		}
		public void setJoint_dims(List<List<String>> joint_dims) {
			this.joint_dims = joint_dims;
		}
	}

	public List<String> getIncludes() {
		return includes;
	}

	public void setIncludes(List<String> includes) {
		this.includes = includes;
	}

	public Select_rule getSelect_rule() {
		return select_rule;
	}

	public void setSelect_rule(Select_rule select_rule) {
		this.select_rule = select_rule;
	}
	
}
