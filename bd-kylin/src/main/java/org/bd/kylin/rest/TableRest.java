package org.bd.kylin.rest;

import org.bd.kylin.RestRequstHandle;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 获取hive表信息<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午8:57:10 |创建
 */
public class TableRest extends RestRequstHandle{
	
	/**
	 * <b>描述：</b> 获取hive所有数据库
	 * @author wpk | 2017年11月21日 上午8:58:49 |创建
	 * @return String
	 */
	public static String showHiveDatabases(){
		String para = "tables/hive";
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 获取hive指定库下的所有表
	 * @author wpk | 2017年11月21日 上午9:01:00 |创建
	 * @param database	数据库名
	 * @return String
	 */
	public static String showHiveTables(String database){
		String para = "tables/hive/"+database;
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 获取kylin上存在的所有hive表
	 * @author wpk | 2017年11月21日 上午9:07:04 |创建
	 * @param project	kylin指定项目名
	 * @return String
	 */
	public static String getTableDesc(String project){
		return getTableDesc(project, false);
	}
	
	/**
	 * <b>描述：</b> 获取kylin上存在的所有hive表
	 * @author wpk | 2017年11月21日 上午9:33:18 |创建
	 * @param project	kylin指定项目名
	 * @param ext		表存储的详细信息，true表示展示，false表示隐藏
	 * @return String
	 */
	public static String getTableDesc(String project, boolean ext){
		String para = "tables?project="+project+"&ext="+ext;
		String result = request(para, RequestMethod.GET);
		return result;
	}
}
