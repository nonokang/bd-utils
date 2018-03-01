package org.bd.datax.hdfs;

import java.util.List;

import org.bd.datax.bean.Write;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> mysql写入配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月20日 下午9:29:52 |创建
 */
public class HdfsWrite extends Write{

	private String name = "hdfswriter";
	private WriteParameter parameter;

	public class WriteParameter{
		private String defaultFS;//必填-hdfs文件系统namenode节点地址
		private String fileType;//必填-文件类型（text、orc）
		private String path;//必填-文件路径
		private String fileName;//必填-文件名
		private List<Object> column;//必填-采集列（必须一一对应）
		private String writeMode;//必填-写入模式
		private String fieldDelimiter;//必填-字段分隔符
		private String compress;//文件压缩类型，默认”NONE“
		private String encoding;//编码配置，默认”UTF-8“
		private Boolean haveKerberos;//是否有Kerberos认证，默认”false“
		private String kerberosKeytabFilePath;//Kerberos认证 keytab文件路径，绝对路径
		private String kerberosPrincipal;//Kerberos认证Principal名
		
		public class WriteColumn{
			private String name;
			private String type;
			
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public String getType() {
				return type;
			}
			public void setType(String type) {
				this.type = type;
			}
			
		}

		public String getDefaultFS() {
			return defaultFS;
		}

		public void setDefaultFS(String defaultFS) {
			this.defaultFS = defaultFS;
		}

		public String getFileType() {
			return fileType;
		}

		public void setFileType(String fileType) {
			this.fileType = fileType;
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

		public List<Object> getColumn() {
			return column;
		}

		public void setColumn(List<Object> column) {
			this.column = column;
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

		public String getCompress() {
			return compress;
		}

		public void setCompress(String compress) {
			this.compress = compress;
		}

		public String getEncoding() {
			return encoding;
		}

		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}

		public Boolean getHaveKerberos() {
			return haveKerberos;
		}

		public void setHaveKerberos(Boolean haveKerberos) {
			this.haveKerberos = haveKerberos;
		}

		public String getKerberosKeytabFilePath() {
			return kerberosKeytabFilePath;
		}

		public void setKerberosKeytabFilePath(String kerberosKeytabFilePath) {
			this.kerberosKeytabFilePath = kerberosKeytabFilePath;
		}

		public String getKerberosPrincipal() {
			return kerberosPrincipal;
		}

		public void setKerberosPrincipal(String kerberosPrincipal) {
			this.kerberosPrincipal = kerberosPrincipal;
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
