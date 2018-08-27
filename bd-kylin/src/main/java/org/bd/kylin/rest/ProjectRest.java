package org.bd.kylin.rest;

import org.bd.kylin.RestRequstHandle;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> kylin项目接口<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 下午2:37:07 |创建
 */
public class ProjectRest extends RestRequstHandle{

	/**
	 * <b>描述：</b> 获取项目信息
	 * @author wpk | 2017年11月21日 下午2:38:51 |创建
	 * @return String
	 */
	public static String getProjects(){
		String para = "projects";
		String result = request(para, RequestMethod.GET);
		return result;
	}
}
