package org.bd.kylin.rest;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bd.kylin.RestRequstHandle;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> model请求rest接口类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月30日 下午2:33:52 |创建
 */
public class ModelRest extends RestRequstHandle{

    private static final Logger logger = Logger.getLogger(ModelRest.class);

	/**
	 * <b>描述：</b> 通过模型名称获取模型信息
	 * @author wpk | 2017年10月30日 下午2:36:25 |创建
	 * @param modelName
	 * @throws Exception
	 * @return String
	 */
	public static String getModel(String modelName) {
		String para = "model/"+modelName;
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 提交模型
	 * @author wpk | 2017年10月30日 下午2:36:34 |创建
	 * @param json
	 * @throws Exception
	 * @return String
	 */
	public static String submitModel(String json) {
		String para = "models/";
		String result = request(para, RequestMethod.POST, json);
		return result;
	}

	/**
	 * <b>描述：</b> 删除model（注意，先删除model下的所有cube后再删除model）
	 * @author wpk | 2017年10月30日 下午2:36:58 |创建
	 * @param modelName
	 * @throws Exception
	 * @return String
	 */
	public static void cancelModel(String modelName) {
		String para = "models/"+modelName;
		try {
			request(para, RequestMethod.DELETE);
		} catch (Exception e) {
			String str = "Data Model with name "+modelName+" not found";
			if(!e.getMessage().contains(str)){
				logger.equals(e.getMessage());
			}
		}
	}
	
	/**
	 * <b>描述：</b> 获取kylin上所有的模型
	 * @author wpk | 2017年11月21日 上午10:00:09 |创建
	 * @return String
	 */
	public static String getModels(){
		return getModels(null);
	}
	
	/**
	 * <b>描述：</b> 获取指定项目下的所有模型
	 * @author wpk | 2017年11月21日 上午10:01:22 |创建
	 * @param project
	 * @return String
	 */
	public static String getModels(String project){
		StringBuilder para = new StringBuilder();
		para.append("models");
		if(StringUtils.isNotBlank(project)){
			para.append("?project=").append(project);
		}
		String result = request(para.toString(), RequestMethod.GET);
		return result;
	}
	
}
