package org.bd.datax.hdfs;

import java.util.List;

import org.bd.datax.bean.Read;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> hdfs读取配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午12:10:08 |创建
 */
public class HdfsRead extends Read{

	private String name = "hdfsreader";
	private Parameter parameter;
	
	public class Parameter{
		private String path;//必填-文件路径
		private String defaultFS;//必填-hdfs文件系统namenode节点地址
		private String fileType;//必填-文件类型，目前只支持用户配置为"text"、"orc"、"rc"、"seq"、"csv"
		private List<Object> column;//必填-字段列表，默认”["*"]“，全部按照string类型读取
		private String fieldDelimiter;//字段分隔符，默认”,“
		private String encoding;//默认”UTF-8“
		private String nullFormat;//定义哪些字符串可以表示为null
		private Boolean haveKerberos;//是否有Kerberos认证，默认”false“
		private String kerberosKeytabFilePath;//Kerberos认证 keytab文件路径，绝对路径
		private String kerberosPrincipal;//Kerberos认证Principal名
		private String compress;//文件压缩方式
		private CsvReaderConfig csvReaderConfig;//读取CSV类型文件参数配置
		
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
		
		public class CsvReaderConfig{
			private Boolean caseSensitive = true;
			private char textQualifier = 34;
			private Boolean trimWhitespace = true;
			private Boolean useTextQualifier = true;//是否使用csv转义字符
			private char delimiter = 44;//分隔符
			private char recordDelimiter = 0;
			private char comment = 35;
			private Boolean useComments = true;
			private Integer escapeMode = 1;
			private Boolean safetySwitch = true;//单列长度是否限制100000字符
			private Boolean skipEmptyRecords = true;//是否跳过空行
			private Boolean captureRawRecord = true;
			
			public Boolean getCaseSensitive() {
				return caseSensitive;
			}
			public void setCaseSensitive(Boolean caseSensitive) {
				this.caseSensitive = caseSensitive;
			}
			public char getTextQualifier() {
				return textQualifier;
			}
			public void setTextQualifier(char textQualifier) {
				this.textQualifier = textQualifier;
			}
			public Boolean getTrimWhitespace() {
				return trimWhitespace;
			}
			public void setTrimWhitespace(Boolean trimWhitespace) {
				this.trimWhitespace = trimWhitespace;
			}
			public Boolean getUseTextQualifier() {
				return useTextQualifier;
			}
			public void setUseTextQualifier(Boolean useTextQualifier) {
				this.useTextQualifier = useTextQualifier;
			}
			public char getDelimiter() {
				return delimiter;
			}
			public void setDelimiter(char delimiter) {
				this.delimiter = delimiter;
			}
			public char getRecordDelimiter() {
				return recordDelimiter;
			}
			public void setRecordDelimiter(char recordDelimiter) {
				this.recordDelimiter = recordDelimiter;
			}
			public char getComment() {
				return comment;
			}
			public void setComment(char comment) {
				this.comment = comment;
			}
			public Boolean getUseComments() {
				return useComments;
			}
			public void setUseComments(Boolean useComments) {
				this.useComments = useComments;
			}
			public Integer getEscapeMode() {
				return escapeMode;
			}
			public void setEscapeMode(Integer escapeMode) {
				this.escapeMode = escapeMode;
			}
			public Boolean getSafetySwitch() {
				return safetySwitch;
			}
			public void setSafetySwitch(Boolean safetySwitch) {
				this.safetySwitch = safetySwitch;
			}
			public Boolean getSkipEmptyRecords() {
				return skipEmptyRecords;
			}
			public void setSkipEmptyRecords(Boolean skipEmptyRecords) {
				this.skipEmptyRecords = skipEmptyRecords;
			}
			public Boolean getCaptureRawRecord() {
				return captureRawRecord;
			}
			public void setCaptureRawRecord(Boolean captureRawRecord) {
				this.captureRawRecord = captureRawRecord;
			}
			
		}

		public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}

		public String getDefaultFS() {
			return defaultFS;
		}

		public void setDefaultFS(String defaultFS) {
			this.defaultFS = defaultFS;
		}

		public List<Object> getColumn() {
			return column;
		}

		public void setColumn(List<Object> column) {
			this.column = column;
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

		public String getFileType() {
			return fileType;
		}

		public void setFileType(String fileType) {
			this.fileType = fileType;
		}

		public String getNullFormat() {
			return nullFormat;
		}

		public void setNullFormat(String nullFormat) {
			this.nullFormat = nullFormat;
		}

		public String getCompress() {
			return compress;
		}

		public void setCompress(String compress) {
			this.compress = compress;
		}

		public CsvReaderConfig getCsvReaderConfig() {
			return csvReaderConfig;
		}

		public void setCsvReaderConfig(CsvReaderConfig csvReaderConfig) {
			this.csvReaderConfig = csvReaderConfig;
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

	public Parameter getParameter() {
		return parameter;
	}

	public void setParameter(Parameter parameter) {
		this.parameter = parameter;
	}
	
}
