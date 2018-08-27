package org.bd.kylin.rest;

import org.apache.log4j.Logger;
import org.bd.kylin.RestRequstHandle;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> cube请求rest接口类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月26日 上午9:40:43 |创建
 */
public class CubeRest extends RestRequstHandle{
	
    private static final Logger logger = Logger.getLogger(CubeRest.class);

	/**
	 * <b>描述：</b> 获取所有cube信息（不是详细信息）
	 * @author wpk | 2017年9月26日 上午9:42:20 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String getCubes() {
		String para = "cubes";
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 通过cube名称获取cube信息
	 * @author wpk | 2017年9月26日 上午9:44:20 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String getCube(String cubeName) {
		String para = "cubes/"+ cubeName;
		String result = request(para, RequestMethod.GET);
		return result;
	}

	/**
	 * <b>描述：</b> 通过cube名称获取cube详细信息
	 * @author wpk | 2017年9月26日 上午9:44:20 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String getCubeDesc(String cubeName) {
		String para = "cube_desc/" + cubeName + "/desc";
		String result = request(para, RequestMethod.GET);
		return result;
	}

	/**
	 * <b>描述：</b> 启用cube
	 * @author wpk | 2017年9月26日 上午10:40:38 |创建
	 * @param cubeName
	 * @throws Exception
	 * @return String
	 */
	public static String enableCube(String cubeName) {
		String para = "/cubes/"+cubeName+"/enable";
		String result = request(para, RequestMethod.PUT);
		return result;
	}

	/**
	 * <b>描述：</b> 禁用cube
	 * @author wpk | 2017年9月26日 上午10:47:06 |创建
	 * @param cubeName
	 * @throws Exception
	 * @return String
	 */
	public static String disableCube(String cubeName) {
		String para = "/cubes/"+cubeName+"/disable";
		String result = request(para, RequestMethod.PUT);
		return result;
	}

	/**
	 * <b>描述：</b> 清除cube
	 * @author wpk | 2017年9月26日 上午10:49:28 |创建
	 * @param cubeName
	 * @throws Exception
	 * @return String
	 */
	public static String purgeCube(String cubeName) {
		String para = "/cubes/"+cubeName+"/purge";
		String result = request(para, RequestMethod.PUT);
		return result;
	}

	/**
	 * <b>描述：</b> 重新创建cube
	 * @author wpk | 2017年9月26日 上午10:51:51 |创建
	 * @param cubeName
	 * @throws Exception
	 * @return String
	 */
	public static String buildCube(String cubeName, String json) {
		String para = "cubes/"+cubeName+"/rebuild";
		String result = request(para, RequestMethod.PUT, json);
		return result;
	}

	/**
	 * <b>描述：</b> 提交cube
	 * @author wpk | 2017年9月26日 上午11:34:58 |创建
	 * @param json
	 * @throws Exception
	 * @return String
	 */
	public static String submitCube(String json) {
		String para = "cubes/";
		String result = request(para, RequestMethod.POST, json);
		return result;
	}

	/**
	 * <b>描述：</b> 删除cube（注意:删除cube将同时删除cube下的所有job）
	 * @author wpk | 2017年10月27日 下午3:12:29 |创建
	 * @param cubeName
	 * @throws Exception
	 * @return String
	 */
	public static void cancelCube(String cubeName) {
		String para = "cubes/"+cubeName;
		try {
			request(para, RequestMethod.DELETE);
		} catch (Exception e) {
			String str = "Cube with name "+cubeName+" not found";
			if(!e.getMessage().contains(str)){
				logger.equals(e.getMessage());
			}
		}
	}
	
	/**
	 * <b>描述：</b> 获取Habse信息
	 * @author wpk | 2017年11月20日 下午5:23:28 |创建
	 * @param cubeName
	 * @return String
	 */
	public static String getHBaseInfo(String cubeName){
		String para = "cubes/" + cubeName + "/hbase";
		String result = request(para, RequestMethod.GET);
		return result;
	}
}