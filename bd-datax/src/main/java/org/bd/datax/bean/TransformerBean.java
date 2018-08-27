package org.bd.datax.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;

import org.bd.datax.bean.Transformer.Parameter;
import org.bd.datax.DataxException;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> Transformer转换实体类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月15日 上午11:01:30 |创建
 */
@SuppressWarnings("rawtypes")
public class TransformerBean {

	public final static String column = "column";
	public final static String code = "code";
	public final static String conf = "字段转换配置";

	/**
	 * <b>描述：</b> 获取转换对象
	 * @author wpk | 2017年10月15日 上午11:02:29 |创建
	 * @param map
	 * @return Transformer
	 */
	public Transformer getBean(Map map){
		Transformer bean = new Transformer();
		Parameter para = bean.new Parameter();
		if(map.containsKey(column) && map.get(column) != null){
			if(map.get(column) instanceof JSONArray){
				JSONArray ja = (JSONArray)map.get(column);
				String code = getCode(ja);
				if(null == code){
					throw new DataxException(String.format("%s【%s】不存在或者为空值", conf, code));
				}
				para.setCode(code);
				List<String> list = new ArrayList<String>();
				list.add("import groovy.json.JsonSlurper;");
				para.setExtraPackage(list);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", conf, column, JSONArray.class));
			}
		}
		
		bean.setName("dx_groovy");
		bean.setParameter(para);
		return bean;
	}
	
	/**
	 * <b>描述：</b> 获取code
	 * @author wpk | 2017年10月15日 上午11:18:48 |创建
	 * @param ja
	 * @return String
	 */
	private String getCode(JSONArray ja){
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<ja.length();i++){
			sb.append("record.setColumn(").append(i).append(",new StringColumn(GroovyTransformerStaticUtil.replaceAll(");
			sb.append("record.getColumn(").append(i).append(").asString()");
			sb.append(")));");
		}
		if(sb.length()>0){
			sb.append("return record;");
			return sb.toString();
		}
		return null;
	}

}
