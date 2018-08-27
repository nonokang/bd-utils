package org.bd.datax.bean;

import java.util.List;


/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> datax字段处理类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月20日 下午9:47:30 |创建
 */
public class Transformer {

	private String name;
	private Parameter parameter;
	
	public class Parameter{
		private List<String> extraPackage;
		private String code;
		
		public List<String> getExtraPackage() {
			return extraPackage;
		}
		public void setExtraPackage(List<String> extraPackage) {
			this.extraPackage = extraPackage;
		}
		public String getCode() {
			return code;
		}
		public void setCode(String code) {
			this.code = code;
		}
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Parameter getParameter() {
		return parameter;
	}

	public void setParameter(Parameter parameter) {
		this.parameter = parameter;
	}
	
}
