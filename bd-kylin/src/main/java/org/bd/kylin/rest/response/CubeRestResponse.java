package org.bd.kylin.rest.response;

import java.io.IOException;
import java.util.List;

import org.bd.kylin.CubeBuildTypeEnum;
import org.bd.kylin.request.JobBuildRequest;
import org.bd.kylin.response.HBaseResponse;
import org.bd.kylin.rest.CubeRest;
import org.bd.kylin.utils.JsonBinder;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> cube请求rest接口类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 下午3:02:48 |创建
 */
public class CubeRestResponse extends CubeRest{
	
//    private static final Logger logger = Logger.getLogger(CubeRestResponse.class);

	/**
	 * <b>描述：</b> 获取所有cube信息（不是详细信息）
	 * @author wpk | 2017年11月21日 下午3:03:35 |创建
	 * @return String
	 */
	public static String getCubesResp() {
		return getCubes();
	}
	
	/**
	 * <b>描述：</b> 通过cube名称获取cube信息
	 * @author wpk | 2017年11月21日 下午3:04:08 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String getCubeResp(String cubeName) {
		return getCube(cubeName);
	}

	/**
	 * <b>描述：</b> 通过cube名称获取cube详细信息
	 * @author wpk | 2017年11月21日 下午3:05:03 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String getCubeDescResp(String cubeName) {
		return getCubeDesc(cubeName);
	}

	/**
	 * <b>描述：</b> 启用cube
	 * @author wpk | 2017年11月21日 下午3:05:57 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String enableCubeResp(String cubeName) {
		return enableCube(cubeName);
	}

	/**
	 * <b>描述：</b> 禁用cube
	 * @author wpk | 2017年11月21日 下午3:06:41 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String disableCubeResp(String cubeName) {
		return disableCube(cubeName);
	}

	/**
	 * <b>描述：</b> 清除cube
	 * @author wpk | 2017年11月21日 下午3:06:57 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String purgeCubeResp(String cubeName) {
		return purgeCube(cubeName);
	}

	/**
	 * <b>描述：</b> 重新创建cube
	 * @author wpk | 2017年11月21日 下午3:10:51 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String buildCubeResp(String cubeName) {
		JobBuildRequest jbr = new JobBuildRequest();
		jbr.setStartTime(19700101000000l);
		jbr.setEndTime(2922789940817071255l);
		jbr.setBuildType(CubeBuildTypeEnum.BUILD.name());
		
		JSONObject jo = new JSONObject(jbr);//对象转json
		
		return buildCube(cubeName, jo.toString());
	}

	/**
	 * <b>描述：</b> 提交cube
	 * @author wpk | 2017年11月21日 下午3:11:24 |创建
	 * @param json
	 * @return String
	 */
	public static String submitCubeResp(String json) {
		return submitCube(json);
	}

	/**
	 * <b>描述：</b> 删除cube（注意:删除cube将同时删除cube下的所有job）
	 * @author wpk | 2017年11月21日 下午3:11:58 |创建
	 * @param cubeName
	 * @return void
	 */
	public static void cancelCubeResp(String cubeName) {
		cancelCube(cubeName);
	}
	
	/**
	 * <b>描述：</b> 获取Habse信息
	 * @author wpk | 2017年11月21日 下午3:13:10 |创建
	 * @param cubeName
	 * @return List<HBaseResponse>
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonParseException 
	 */
	@SuppressWarnings("unchecked")
	public static List<HBaseResponse> getHBaseInfoResp(String cubeName) throws JsonParseException, JsonMappingException, IOException{
		String result = getHBaseInfo(cubeName);
		ObjectMapper om = JsonBinder.buildNormalBinder().getMapper();
		List<HBaseResponse> list = (List<HBaseResponse>)om.readValue(result, new TypeReference<List<HBaseResponse>>() {});
		return list;
	}
}