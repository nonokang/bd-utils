package org.bd.datax.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;

import org.bd.datax.DataxBean;
import org.bd.datax.DataxException;
import org.bd.datax.oracle.OracleRead.Parameter;
import org.bd.datax.oracle.OracleRead.Parameter.Connection;
import org.bd.datax.oracle.OracleWrite.WriteParameter;
import org.bd.datax.oracle.OracleWrite.WriteParameter.WriteConnection;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> oracle脚本转换类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午4:20:47 |创建
 */
@SuppressWarnings("rawtypes")
public class OracleBean implements DataxBean{

	public final static String readconf = "oracle读取配置";
	public final static String writeconf = "oracle写入配置";

	/**
	 * <b>描述：</b> 获取读取对象
	 * @author wpk | 2017年10月13日 下午4:59:48 |创建
	 * @param map
	 * @return OracleRead
	 */
	@Override
	public OracleRead readBean(Map map){
		OracleRead bean = new OracleRead();
		Parameter para = bean.new Parameter();
		List<Connection> connection = new ArrayList<Connection>();
		Connection conn = para.new Connection();

		if(map.containsKey(OraclePara.username) && map.get(OraclePara.username) != null){
			para.setUsername(map.get(OraclePara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, OraclePara.username));
		}
		if(map.containsKey(OraclePara.password) && map.get(OraclePara.password) != null){
			para.setPassword(map.get(OraclePara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, OraclePara.password));
		}
		if(map.containsKey(OraclePara.column) && map.get(OraclePara.column) != null){
			if(map.get(OraclePara.column) instanceof JSONArray){
				List<String> column = new ArrayList<String>();
				JSONArray ja = (JSONArray)map.get(OraclePara.column);
				for(int i=0;i<ja.length();i++){
					column.add(ja.getString(i));
				}
				para.setColumn(column);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", readconf, OraclePara.column, JSONArray.class));
			}
		}else{
			List<String> column = new ArrayList<String>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(OraclePara.splitPk) && map.get(OraclePara.splitPk) != null){
			para.setSplitPk(map.get(OraclePara.splitPk).toString());
		}
		if(map.containsKey(OraclePara.where) && map.get(OraclePara.where) != null){
			para.setWhere(map.get(OraclePara.where).toString());
		}
		if(map.containsKey(OraclePara.fetchSize) && map.get(OraclePara.fetchSize) != null){
			para.setFetchSize(Integer.parseInt(map.get(OraclePara.fetchSize).toString()));
		}
		if(map.containsKey(OraclePara.session) && map.get(OraclePara.session) != null){
			List<String> session = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.session);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				session.add(pa);
			}
			para.setSession(session);
		}

		if(map.containsKey(OraclePara.jdbcUrl) && map.get(OraclePara.jdbcUrl) != null){
			List<String> jdbcUrl = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.jdbcUrl);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				jdbcUrl.add(pa);
			}
			conn.setJdbcUrl(jdbcUrl);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, OraclePara.jdbcUrl));
		}
		if(map.containsKey(OraclePara.table) && map.get(OraclePara.table) != null){
			List<String> table = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.table);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				table.add(pa);
			}
			conn.setTable(table);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, OraclePara.table));
		}
		if(map.containsKey(OraclePara.querySql) && map.get(OraclePara.querySql) != null){
			List<String> querySql = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.querySql);
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
	 * @return OracleWrite
	 */
	@Override
	public OracleWrite writeBean(Map map){
		OracleWrite bean = new OracleWrite();
		WriteParameter para = bean.new WriteParameter();
		List<WriteConnection> connection = new ArrayList<WriteConnection>();
		WriteConnection conn = para.new WriteConnection();
		
		if(map.containsKey(OraclePara.username) && map.get(OraclePara.username) != null){
			para.setUsername(map.get(OraclePara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, OraclePara.username));
		}
		if(map.containsKey(OraclePara.password) && map.get(OraclePara.password) != null){
			para.setPassword(map.get(OraclePara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, OraclePara.password));
		}
		if(map.containsKey(OraclePara.column) && map.get(OraclePara.column) != null){
			if(map.get(OraclePara.column) instanceof JSONArray){
				if(map.get(OraclePara.column) instanceof JSONArray){
					List<String> column = new ArrayList<String>();
					JSONArray ja = (JSONArray)map.get(OraclePara.column);
					for(int i=0;i<ja.length();i++){
						column.add(ja.getString(i));
					}
					para.setColumn(column);
				}
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", writeconf, OraclePara.column, JSONArray.class));
			}
		}else{
			List<String> column = new ArrayList<String>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(OraclePara.preSql) && map.get(OraclePara.preSql) != null){
			List<String> preSql = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.preSql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				preSql.add(pa);
			}
			para.setPreSql(preSql);
		}
		if(map.containsKey(OraclePara.postSql) && map.get(OraclePara.postSql) != null){
			List<String> postSql = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.postSql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				postSql.add(pa);
			}
			para.setPostSql(postSql);
		}
		if(map.containsKey(OraclePara.batchSize) && map.get(OraclePara.batchSize) != null){
			para.setBatchSize(Integer.parseInt(map.get(OraclePara.batchSize).toString()));
		}
		if(map.containsKey(OraclePara.session) && map.get(OraclePara.session) != null){
			List<String> session = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.session);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				session.add(pa);
			}
			para.setSession(session);
		}
		
		if(map.containsKey(OraclePara.table) && map.get(OraclePara.table) != null){
			List<String> table = new ArrayList<String>();
			String strs = (String)map.get(OraclePara.table);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				table.add(pa);
			}
			conn.setTable(table);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, OraclePara.table));
		}
		if(map.containsKey(OraclePara.jdbcUrl) && map.get(OraclePara.jdbcUrl) != null){
			conn.setJdbcUrl(map.get(OraclePara.jdbcUrl).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, OraclePara.jdbcUrl));
		}
		connection.add(conn);
		para.setConnection(connection);
		
		bean.setParameter(para);
		return bean;
	}
	
}
