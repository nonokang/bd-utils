package org.bd.datax.mysql;

import java.util.List;

import org.bd.datax.bean.Write;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> mysql写入配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月20日 下午9:29:52 |创建
 */
public class MysqlWrite extends Write{

	private String name = "mysqlwriter";
	private WriteParameter parameter;

	public class WriteParameter{
		private String username;//必填-用户名
		private String password;//必填-密码
		private String writeMode = "insert";//必填-写入模式，默认”insert“，可选择（insert/replace/update）
		//目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
		//如果要依次写入全部列，使用*表示, 例如: "column": ["*"]
		private List<String> column;//必填
		private List<String> preSql;//写入前执行的语句
		private List<String> postSql;//写入后执行的语句
		private Integer batchSize;//一次性批量提交的记录数大小，默认”1024“
		private List<String> session;
		private List<WriteConnection> connection;//必填
		
		public class WriteConnection{
			private String jdbcUrl;//必填-jdbc连接信息
			private List<String> table;//必填-同步表
			
			public String getJdbcUrl() {
				return jdbcUrl;
			}
			public void setJdbcUrl(String jdbcUrl) {
				this.jdbcUrl = jdbcUrl;
			}
			public List<String> getTable() {
				return table;
			}
			public void setTable(List<String> table) {
				this.table = table;
			}
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
		public String getWriteMode() {
			return writeMode;
		}
		public void setWriteMode(String writeMode) {
			this.writeMode = writeMode;
		}
		public List<String> getColumn() {
			return column;
		}
		public void setColumn(List<String> column) {
			this.column = column;
		}
		public List<String> getPreSql() {
			return preSql;
		}
		public void setPreSql(List<String> preSql) {
			this.preSql = preSql;
		}
		public List<String> getPostSql() {
			return postSql;
		}
		public void setPostSql(List<String> postSql) {
			this.postSql = postSql;
		}
		public Integer getBatchSize() {
			return batchSize;
		}
		public void setBatchSize(Integer batchSize) {
			this.batchSize = batchSize;
		}
		public List<String> getSession() {
			return session;
		}
		public void setSession(List<String> session) {
			this.session = session;
		}
		public List<WriteConnection> getConnection() {
			return connection;
		}
		public void setConnection(List<WriteConnection> connection) {
			this.connection = connection;
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
