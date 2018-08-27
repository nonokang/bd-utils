package org.bd.datax.oracle;

import java.util.List;

import org.bd.datax.bean.Read;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> oracle读取配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 上午11:05:19 |创建
 */
public class OracleRead extends Read{

	private String name = "oraclereader";
	private Parameter parameter;
	
	public class Parameter{
		private String username;//必填-用户名
		private String password;//必填-密码
		//目的表需要读取数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。
		//如果要依次读取全部列，使用*表示, 例如: "column": ["*"]
		private List<String> column;//必填-获取所有列，使用”['*']“
		private String splitPk;//对字段进行分片，仅支持整数型、字符串型的字段
		private String where;//筛选条件
		private Integer fetchSize;//每次批量数据获取条数，默认”1024“
		private List<String> session;
		private List<Connection> connection;//必填
		
		public class Connection{
			private List<String> jdbcUrl;//必填-JDBC连接信息
			private List<String> table;//必填-同步表
			private List<String> querySql;//自定义执行语句，注意（当用户配置querySql时，直接忽略table、column、where条件的配置）

			public List<String> getJdbcUrl() {
				return jdbcUrl;
			}
			public void setJdbcUrl(List<String> jdbcUrl) {
				this.jdbcUrl = jdbcUrl;
			}
			public List<String> getTable() {
				return table;
			}
			public void setTable(List<String> table) {
				this.table = table;
			}
			public List<String> getQuerySql() {
				return querySql;
			}
			public void setQuerySql(List<String> querySql) {
				this.querySql = querySql;
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
		public List<String> getColumn() {
			return column;
		}
		public void setColumn(List<String> column) {
			this.column = column;
		}
		public String getSplitPk() {
			return splitPk;
		}
		public void setSplitPk(String splitPk) {
			this.splitPk = splitPk;
		}
		public String getWhere() {
			return where;
		}
		public void setWhere(String where) {
			this.where = where;
		}
		public Integer getFetchSize() {
			return fetchSize;
		}
		public void setFetchSize(Integer fetchSize) {
			this.fetchSize = fetchSize;
		}
		public List<String> getSession() {
			return session;
		}
		public void setSession(List<String> session) {
			this.session = session;
		}
		public List<Connection> getConnection() {
			return connection;
		}
		public void setConnection(List<Connection> connection) {
			this.connection = connection;
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
