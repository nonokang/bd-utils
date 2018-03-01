package org.bd.kylin.rest;

import org.apache.commons.lang3.StringUtils;
import org.bd.kylin.CubeException;
import org.bd.kylin.RestRequstHandle;
import org.bd.kylin.request.SQLRequest;
import org.bd.kylin.utils.JsonBinder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> kylin语句查询<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午9:15:43 |创建
 */
public class QueryRest extends RestRequstHandle{

	/**
	 * <b>描述：</b> 获取kylin上的所有表元数据信息<br>
	 * 所有表指的是用于创建模型和cube的表
	 * @author wpk | 2017年11月21日 上午9:19:29 |创建
	 * @return String
	 */
	public static String getMetadatas(String project){
		String para = "tables_and_columns?project="+project;
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 查询语句
	 * @author wpk | 2017年11月21日 下午3:56:10 |创建
	 * @return String
	 * @throws JsonProcessingException 
	 */
	public static String query(SQLRequest sr) throws JsonProcessingException{
		String para = "query";
		if(null == sr){
			throw new CubeException("传入[SQLRequest]对象为空!");
		}
		if(StringUtils.isBlank(sr.getProject())){
			throw new CubeException("传入[project]参数为空!");
		}
		if(StringUtils.isBlank(sr.getSql())){
			throw new CubeException("传入[sql]参数为空!");
		}
		
		ObjectMapper om = JsonBinder.buildNonNullBinder().getMapper();
		String json = om.writeValueAsString(sr);
		
		String result = request(para, RequestMethod.POST, json);
		return result;
	}
	
	/**
	 * <b>描述：</b> 查询语句
	 * @author wpk | 2017年11月21日 下午5:24:25 |创建
	 * @param project
	 * @param sql
	 * @throws JsonProcessingException
	 * @return String
	 */
	public static String query(String project, String sql) throws JsonProcessingException{
		SQLRequest item = new SQLRequest();
		item.setProject(project);
		item.setSql(sql);
		return query(item);
	}
	
}
