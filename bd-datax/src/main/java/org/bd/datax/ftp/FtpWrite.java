package org.bd.datax.ftp;

import java.util.List;

import org.bd.datax.bean.Write;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> ftp写入配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午12:01:31 |创建
 */
public class FtpWrite extends Write{

	private String name = "ftpwriter";
	private WriteParameter parameter;
	
	public class WriteParameter{
		private String protocol;//必填-服务器协议，目前支持传输协议有ftp和sftp
		private String host;//必填-服务器地址
		private Integer port;//若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21
		private Integer timeout;//连接服务器连接超时时间，单位毫秒，默认”60000“
		private String username;//必填-服务器访问用户名
		private String password;//必填-服务器访问密码
		private String path;//必填-文件路径
		private String fileName;//必填-文件名
		private String writeMode;//必填-写入前数据清理处理模式
		private String fieldDelimiter;//字段分隔符，默认”,“
		private String encoding;//文件的编码配置，默认”UTF-8“
		private String nullFormat;//定义哪些字符串可以表示为null，默认”\N“
		private String dateFormat;//日期类型的数据序列化到文件中时的格式
		private String fileFormat;//文件写出的格式，默认“text”
		private List<String> header;//txt写出时的表头，示例['id', 'name', 'age']
		
		public String getProtocol() {
			return protocol;
		}
		public void setProtocol(String protocol) {
			this.protocol = protocol;
		}
		public String getHost() {
			return host;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public Integer getPort() {
			return port;
		}
		public void setPort(Integer port) {
			this.port = port;
		}
		public Integer getTimeout() {
			return timeout;
		}
		public void setTimeout(Integer timeout) {
			this.timeout = timeout;
		}
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		public String getWriteMode() {
			return writeMode;
		}
		public void setWriteMode(String writeMode) {
			this.writeMode = writeMode;
		}
		public String getFieldDelimiter() {
			return fieldDelimiter;
		}
		public void setFieldDelimiter(String fieldDelimiter) {
			this.fieldDelimiter = fieldDelimiter;
		}
		public String getEncoding() {
			return encoding;
		}
		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}
		public String getNullFormat() {
			return nullFormat;
		}
		public void setNullFormat(String nullFormat) {
			this.nullFormat = nullFormat;
		}
		public String getDateFormat() {
			return dateFormat;
		}
		public void setDateFormat(String dateFormat) {
			this.dateFormat = dateFormat;
		}
		public String getFileFormat() {
			return fileFormat;
		}
		public void setFileFormat(String fileFormat) {
			this.fileFormat = fileFormat;
		}
		public List<String> getHeader() {
			return header;
		}
		public void setHeader(List<String> header) {
			this.header = header;
		}
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public WriteParameter getParameter() {
		return parameter;
	}

	public void setParameter(WriteParameter parameter) {
		this.parameter = parameter;
	}
	
}
