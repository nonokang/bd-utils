package org.bd.datax.ftp;

import java.util.List;

import org.bd.datax.bean.Read;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> ftp读取配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 上午11:48:35 |创建
 */
public class FtpRead extends Read{

	private String name = "ftpreader";
	private Parameter parameter;
	
	public class Parameter{
		private String protocol;//必填-服务器协议，目前支持传输协议有ftp和sftp
		private String host;//必填-服务器地址
		private Integer port;//若传输协议是sftp协议，默认值是22；若传输协议是标准ftp协议，默认值是21
		private Integer timeout;//连接服务器连接超时时间，单位毫秒，默认”60000“
		private String connectPattern;//默认”PASV“
		private String username;//必填-服务器访问用户名
		private String password;//必填-服务器访问密码
		private List<String> path;//必填-文件路径
		private List<Object> column;//必填-默认值”['*']“，全部按照string类型读取
		private String fieldDelimiter = ",";//必填-字段分隔符 ，默认”,“
		private String compress;//文本压缩类型，默认不填写意味着没有压缩。支持压缩类型为zip、gzip、bzip2
		private String encoding;//编码配置,默认”UTF-8“
		private Boolean skipHeader;//类CSV格式文件可能存在表头为标题情况，需要跳过。默认”false“
		private String nullFormat;//定义哪些字符串可以表示为null,默认”\N“
		private Integer maxTraversalLevel;//允许遍历文件夹的最大层数，默认”100“
		
		public class ColumnIndex{
			private Integer index;
			private String type;
			
			public Integer getIndex() {
				return index;
			}
			public void setIndex(Integer index) {
				this.index = index;
			}
			public String getType() {
				return type;
			}
			public void setType(String type) {
				this.type = type;
			}
		}
		
		public class ColumnValue{
			private String value;
			private String type;
			
			public String getValue() {
				return value;
			}
			public void setValue(String value) {
				this.value = value;
			}
			public String getType() {
				return type;
			}
			public void setType(String type) {
				this.type = type;
			}
		}

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

		public List<Object> getColumn() {
			return column;
		}

		public void setColumn(List<Object> column) {
			this.column = column;
		}

		public List<String> getPath() {
			return path;
		}

		public void setPath(List<String> path) {
			this.path = path;
		}

		public String getEncoding() {
			return encoding;
		}

		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}

		public String getFieldDelimiter() {
			return fieldDelimiter;
		}

		public void setFieldDelimiter(String fieldDelimiter) {
			this.fieldDelimiter = fieldDelimiter;
		}

		public String getConnectPattern() {
			return connectPattern;
		}

		public void setConnectPattern(String connectPattern) {
			this.connectPattern = connectPattern;
		}

		public String getCompress() {
			return compress;
		}

		public void setCompress(String compress) {
			this.compress = compress;
		}

		public Boolean getSkipHeader() {
			return skipHeader;
		}

		public void setSkipHeader(Boolean skipHeader) {
			this.skipHeader = skipHeader;
		}

		public String getNullFormat() {
			return nullFormat;
		}

		public void setNullFormat(String nullFormat) {
			this.nullFormat = nullFormat;
		}

		public Integer getMaxTraversalLevel() {
			return maxTraversalLevel;
		}

		public void setMaxTraversalLevel(Integer maxTraversalLevel) {
			this.maxTraversalLevel = maxTraversalLevel;
		}
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Parameter getParameter() {
		return parameter;
	}

	public void setParameter(Parameter parameter) {
		this.parameter = parameter;
	}
	
}
