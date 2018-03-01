package org.bd.datax.hdfs;

import org.bd.datax.DataxPara;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> HDFS键值对参数配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年8月8日 上午10:26:39 |创建
 */
public class HdfsPara extends DataxPara{

	//=======================读写共同参数================================
	/** 文件路径*/
	public final static String path = "path";
	/** hdfs文件系统namenode节点地址*/
	public final static String defaultFS = "defaultFS";
	/** 文件类型*/
	public final static String fileType = "fileType";
	/** 列*/
	public final static String column = "column";
	/** 字段分隔符*/
	public final static String fieldDelimiter = "fieldDelimiter";
	/** 当前fileType（文件类型）为csv下的文件压缩方式*/
	public final static String compress = "compress";
	/** 文件编码*/
	public final static String encoding = "encoding";
	/** 空指针定义*/
	public final static String nullFormat = "nullFormat";
	/** 是否有Kerberos认证*/
	public final static String haveKerberos = "haveKerberos";
	/** Kerberos认证 keytab文件路径，绝对路径*/
	public final static String kerberosKeytabFilePath = "kerberosKeytabFilePath";
	/** Kerberos认证Principal名*/
	public final static String kerberosPrincipal = "kerberosPrincipal";
	
	//======================读取参数=================================
	/** 读取CSV类型文件参数配置*/
	public final static String csvReaderConfig = "csvReaderConfig";

	//=======================写入参数================================
	/** 文件名*/
	public final static String fileName = "fileName";
	/** hdfswriter写入前数据清理处理模式*/
	public final static String writeMode = "writeMode";

}
