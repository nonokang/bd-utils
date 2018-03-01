package org.bd.kylin.rest.response;

import java.io.IOException;
import java.util.List;

import org.bd.kylin.response.ProjectInfo;
import org.bd.kylin.rest.ProjectRest;
import org.bd.kylin.utils.JsonBinder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> kylin项目接口<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 下午3:23:13 |创建
 */
public class ProjectRestResponse extends ProjectRest{

	/**
	 * <b>描述：</b> 获取项目信息
	 * @author wpk | 2017年11月21日 下午3:24:05 |创建
	 * @return List<ProjectInfo>
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	@SuppressWarnings("unchecked")
	public static List<ProjectInfo> getProjectsResp() throws JsonParseException, JsonMappingException, IOException{
		String result = getProjects();
		ObjectMapper om = JsonBinder.buildNonNullBinder().getMapper();
		List<ProjectInfo> list = (List<ProjectInfo>)om.readValue(result, new TypeReference<List<ProjectInfo>>() {});
		return list;
	}
}
