package org.bd.datax.sqlserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;

import org.bd.datax.DataxBean;
import org.bd.datax.DataxException;
import org.bd.datax.sqlserver.SqlServerRead.Parameter;
import org.bd.datax.sqlserver.SqlServerRead.Parameter.Connection;
import org.bd.datax.sqlserver.SqlServerWrite.WriteParameter;
import org.bd.datax.sqlserver.SqlServerWrite.WriteParameter.WriteConnection;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> sqlserver脚本转换类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午5:5:41 |创建
 */
@SuppressWarnings("rawtypes")
public class SqlServerBean implements DataxBean{

	public final static String readconf = "SqlServer读取配置";
	public final static String writeconf = "SqlServer写入配置";

	/**
	 * <b>描述：</b> 获取读取对象
	 * @author wpk | 2017年10月13日 下午5:17:30 |创建
	 * @param map
	 * @return SqlServerRead
	 */
	@Override
	public SqlServerRead readBean(Map map){
		SqlServerRead bean = new SqlServerRead();
		Parameter para = bean.new Parameter();
		List<Connection> connection = new ArrayList<Connection>();
		Connection conn = para.new Connection();

		if(map.containsKey(SqlServerPara.username) && map.get(SqlServerPara.username) != null){
			para.setUsername(map.get(SqlServerPara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, SqlServerPara.username));
		}
		if(map.containsKey(SqlServerPara.password) && map.get(SqlServerPara.password) != null){
			para.setPassword(map.get(SqlServerPara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, SqlServerPara.password));
		}
		if(map.containsKey(SqlServerPara.column) && map.get(SqlServerPara.column) != null){
			if(map.get(SqlServerPara.column) instanceof JSONArray){
				List<String> column = new ArrayList<String>();
				JSONArray ja = (JSONArray)map.get(SqlServerPara.column);
				for(int i=0;i<ja.length();i++){
					column.add(ja.getString(i));
				}
				para.setColumn(column);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", readconf, SqlServerPara.column, JSONArray.class));
			}
		}else{
			List<String> column = new ArrayList<String>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(SqlServerPara.splitPk) && map.get(SqlServerPara.splitPk) != null){
			para.setSplitPk(map.get(SqlServerPara.splitPk).toString());
		}
		if(map.containsKey(SqlServerPara.where) && map.get(SqlServerPara.where) != null){
			para.setWhere(map.get(SqlServerPara.where).toString());
		}
		if(map.containsKey(SqlServerPara.fetchSize) && map.get(SqlServerPara.fetchSize) != null){
			para.setFetchSize(Integer.parseInt(map.get(SqlServerPara.fetchSize).toString()));
		}

		if(map.containsKey(SqlServerPara.jdbcUrl) && map.get(SqlServerPara.jdbcUrl) != null){
			List<String> jdbcUrl = new ArrayList<String>();
			String strs = (String)map.get(SqlServerPara.jdbcUrl);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				jdbcUrl.add(pa);
			}
			conn.setJdbcUrl(jdbcUrl);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, SqlServerPara.jdbcUrl));
		}
		if(map.containsKey(SqlServerPara.table) && map.get(SqlServerPara.table) != null){
			List<String> table = new ArrayList<String>();
			String strs = (String)map.get(SqlServerPara.table);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				table.add(pa);
			}
			conn.setTable(table);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, SqlServerPara.table));
		}
		if(map.containsKey(SqlServerPara.querySql) && map.get(SqlServerPara.querySql) != null){
			List<String> querySql = new ArrayList<String>();
			String strs = (String)map.get(SqlServerPara.querySql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				querySql.add(pa+";");
			}
			conn.setQuerySql(querySql);
		}
		connection.add(conn);
		para.setConnection(connection);
		
		bean.setParameter(para);
		return bean;
	}
	
	/**
	 * <b>描述：</b> 获取写入对象
	 * @author wpk | 2017年10月13日 下午4:53:04 |创建
	 * @param map
	 * @return SqlServerWrite
	 */
	@Override
	public SqlServerWrite writeBean(Map map){
		SqlServerWrite bean = new SqlServerWrite();
		WriteParameter para = bean.new WriteParameter();
		List<WriteConnection> connection = new ArrayList<WriteConnection>();
		WriteConnection conn = para.new WriteConnection();
		
		if(map.containsKey(SqlServerPara.username) && map.get(SqlServerPara.username) != null){
			para.setUsername(map.get(SqlServerPara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, SqlServerPara.username));
		}
		if(map.containsKey(SqlServerPara.password) && map.get(SqlServerPara.password) != null){
			para.setPassword(map.get(SqlServerPara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, SqlServerPara.password));
		}
		if(map.containsKey(SqlServerPara.column) && map.get(SqlServerPara.column) != null){
			if(map.get(SqlServerPara.column) instanceof JSONArray){
				if(map.get(SqlServerPara.column) instanceof JSONArray){
					List<String> column = new ArrayList<String>();
					JSONArray ja = (JSONArray)map.get(SqlServerPara.column);
					for(int i=0;i<ja.length();i++){
						column.add(ja.getString(i));
					}
					para.setColumn(column);
				}
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", writeconf, SqlServerPara.column, JSONArray.class));
			}
		}else{
			List<String> column = new ArrayList<String>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(SqlServerPara.preSql) && map.get(SqlServerPara.preSql) != null){
			List<String> preSql = new ArrayList<String>();
			String strs = (String)map.get(SqlServerPara.preSql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				preSql.add(pa+";");
			}
			para.setPreSql(preSql);
		}
		if(map.containsKey(SqlServerPara.postSql) && map.get(SqlServerPara.postSql) != null){
			List<String> postSql = new ArrayList<String>();
			String strs = (String)map.get(SqlServerPara.postSql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				postSql.add(pa+";");
			}
			para.setPostSql(postSql);
		}
		if(map.containsKey(SqlServerPara.batchSize) && map.get(SqlServerPara.batchSize) != null){
			para.setBatchSize(Integer.parseInt(map.get(SqlServerPara.batchSize).toString()));
		}
		
		if(map.containsKey(SqlServerPara.table) && map.get(SqlServerPara.table) != null){
			List<String> table = new ArrayList<String>();
			String strs = (String)map.get(SqlServerPara.table);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				table.add(pa);
			}
			conn.setTable(table);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, SqlServerPara.table));
		}
		if(map.containsKey(SqlServerPara.jdbcUrl) && map.get(SqlServerPara.jdbcUrl) != null){
			conn.setJdbcUrl(map.get(SqlServerPara.jdbcUrl).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, SqlServerPara.jdbcUrl));
		}
		connection.add(conn);
		para.setConnection(connection);
		
		bean.setParameter(para);
		return bean;
	}
	
}
