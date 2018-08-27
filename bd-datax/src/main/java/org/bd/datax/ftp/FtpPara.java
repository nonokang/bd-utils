package org.bd.datax.ftp;

import org.bd.datax.DataxPara;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> ftp键值对参数配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年8月8日 下午3:12:28 |创建
 */
public class FtpPara extends DataxPara{

	//=======================读写共同参数================================
	/** ftp服务器协议*/
	public final static String protocol = "protocol";
	/** ftp服务器地址*/
	public final static String host = "host";
	/** ftp服务器端口*/
	public final static String port = "port";
	/** 连接ftp服务器连接超时时间*/
	public final static String timeout = "timeout";
	/** ftp服务器访问用户名*/
	public final static String username = "username";
	/** ftp服务器访问密码*/
	public final static String password = "password";
	/** 远程FTP文件系统的路径信息*/
	public final static String path = "path";
	/** 字段分隔符*/
	public final static String fieldDelimiter = "fieldDelimiter";
	/** 文件的编码配置*/
	public final static String encoding = "encoding";
	/** 指定值转为null空字符*/
	public final static String nullFormat = "nullFormat";
	
	//=======================读取参数================================
	/** 连接模式*/
	public final static String connectPattern = "connectPattern";
	/** 读取字段列表*/
	public final static String column = "column";
	/** 文本压缩类型*/
	public final static String compress = "compress";
	/** 跳过文件表头标题*/
	public final static String skipHeader = "skipHeader";
	/** 允许遍历文件夹的最大层数*/
	public final static String maxTraversalLevel = "maxTraversalLevel";
	
	//=======================写入参数================================
	/** 写入文件名*/
	public final static String fileName = "fileName";
	/** 写入模式*/
	public final static String writeMode = "writeMode";
	/** 格式化日期*/
	public final static String dateFormat = "dateFormat";
	/** 文件写出的格式*/
	public final static String fileFormat = "fileFormat";
	/** txt写出时的表头*/
	public final static String header = "header";

}
