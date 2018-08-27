package org.bd.kylin.cube;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 度量<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午12:14:26 |创建
 */
public class Measures {

	private String name;
	private Function function;
	
	public class Function{
		private String expression;
		private String returntype;
		private Parameter parameter;
		
		public String getExpression() {
			return expression;
		}
		public void setExpression(String expression) {
			this.expression = expression;
		}
		public String getReturntype() {
			return returntype;
		}
		public void setReturntype(String returntype) {
			this.returntype = returntype;
		}
		public Parameter getParameter() {
			return parameter;
		}
		public void setParameter(Parameter parameter) {
			this.parameter = parameter;
		}
	}
	
	public class Parameter{
		private String type;
		private String value;
		private String next_parameter = null;
		
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public String getNext_parameter() {
			return next_parameter;
		}
		public void setNext_parameter(String next_parameter) {
			this.next_parameter = next_parameter;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Function getFunction() {
		return function;
	}

	public void setFunction(Function function) {
		this.function = function;
	}
	
}
