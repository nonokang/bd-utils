package org.bd.datax.sqlserver;

import org.bd.datax.DataxPara;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> sqlserver键值对参数配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年8月8日 下午2:38:37 |创建
 */
public class SqlServerPara extends DataxPara{

	//=======================读写共同参数================================
	public final static String connection = "connection";
	/** 连接路径（多条连接以逗号分隔）*/
	public final static String jdbcUrl = "jdbcUrl";
	/** 连接用户名*/
	public final static String username = "username";
	/** 密码*/
	public final static String password = "password";
	/** 表（多个表以逗号分隔）*/
	public final static String table = "table";
	/** 列*/
	public final static String column = "column";
	//=======================读取参数================================
	/** 分片字段*/
	public final static String splitPk = "splitPk";
	/** 筛选条件*/
	public final static String where = "where";
	/** 执行语句，该值将忽略table、column、where条件的配置*/
	public final static String querySql = "querySql";
	/** 定义一次性批量获取数据量，注意该值过大将出现OOM情况*/
	public final static String fetchSize = "fetchSize";
	//=======================写入参数================================
	/** 写入前执行该语句（多条语句以分号分隔）*/
	public final static String preSql = "preSql";
	/** 写入后执行该语句（多条语句以分号分隔）*/
	public final static String postSql = "postSql";
	/** 定义一次性批量写入数据量，注意该值过大将出现OOM情况*/
	public final static String batchSize = "batchSize";

}
