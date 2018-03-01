package org.bd.datax.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;

import org.bd.datax.DataxBean;
import org.bd.datax.DataxException;
import org.bd.datax.mysql.MysqlRead.Parameter;
import org.bd.datax.mysql.MysqlRead.Parameter.Connection;
import org.bd.datax.mysql.MysqlWrite.WriteParameter;
import org.bd.datax.mysql.MysqlWrite.WriteParameter.WriteConnection;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> mysql脚本转换类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午3:21:51 |创建
 */
@SuppressWarnings("rawtypes")
public class MysqlBean implements DataxBean{

	public final static String readconf = "mysql读取配置";
	public final static String writeconf = "mysql写入配置";

	/**
	 * <b>描述：</b> 获取读取对象
	 * @author wpk | 2017年10月13日 下午3:42:54 |创建
	 * @param map
	 * @return MysqlRead
	 */
	@Override
	public MysqlRead readBean(Map map){
		MysqlRead bean = new MysqlRead();
		Parameter para = bean.new Parameter();
		List<Connection> connection = new ArrayList<Connection>();
		Connection conn = para.new Connection();

		if(map.containsKey(MysqlPara.username) && map.get(MysqlPara.username) != null){
			para.setUsername(map.get(MysqlPara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, MysqlPara.username));
		}
		if(map.containsKey(MysqlPara.password) && map.get(MysqlPara.password) != null){
			para.setPassword(map.get(MysqlPara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, MysqlPara.password));
		}
		if(map.containsKey(MysqlPara.column) && map.get(MysqlPara.column) != null){
			if(map.get(MysqlPara.column) instanceof JSONArray){
				List<String> column = new ArrayList<String>();
				JSONArray ja = (JSONArray)map.get(MysqlPara.column);
				for(int i=0;i<ja.length();i++){
					column.add(ja.getString(i));
				}
				para.setColumn(column);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", readconf, MysqlPara.column, JSONArray.class));
			}
		}else{
			List<String> column = new ArrayList<String>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(MysqlPara.splitPk) && map.get(MysqlPara.splitPk) != null){
			para.setSplitPk(map.get(MysqlPara.splitPk).toString());
		}
		if(map.containsKey(MysqlPara.where) && map.get(MysqlPara.where) != null){
			para.setWhere(map.get(MysqlPara.where).toString());
		}

		if(map.containsKey(MysqlPara.jdbcUrl) && map.get(MysqlPara.jdbcUrl) != null){
			List<String> jdbcUrl = new ArrayList<String>();
			String strs = (String)map.get(MysqlPara.jdbcUrl);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				jdbcUrl.add(pa);
			}
			conn.setJdbcUrl(jdbcUrl);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, MysqlPara.jdbcUrl));
		}
		if(map.containsKey(MysqlPara.table) && map.get(MysqlPara.table) != null){
			List<String> table = new ArrayList<String>();
			String strs = (String)map.get(MysqlPara.table);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				table.add(pa);
			}
			conn.setTable(table);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, MysqlPara.table));
		}
		if(map.containsKey(MysqlPara.querySql) && map.get(MysqlPara.querySql) != null){
			List<String> querySql = new ArrayList<String>();
			String strs = (String)map.get(MysqlPara.querySql);
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
	 * @return MysqlWrite
	 */
	@Override
	public MysqlWrite writeBean(Map map){
		MysqlWrite bean = new MysqlWrite();
		WriteParameter para = bean.new WriteParameter();
		List<WriteConnection> connection = new ArrayList<WriteConnection>();
		WriteConnection conn = para.new WriteConnection();
		
		if(map.containsKey(MysqlPara.username) && map.get(MysqlPara.username) != null){
			para.setUsername(map.get(MysqlPara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, MysqlPara.username));
		}
		if(map.containsKey(MysqlPara.password) && map.get(MysqlPara.password) != null){
			para.setPassword(map.get(MysqlPara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, MysqlPara.password));
		}
		if(map.containsKey(MysqlPara.column) && map.get(MysqlPara.column) != null){
			if(map.get(MysqlPara.column) instanceof JSONArray){
				if(map.get(MysqlPara.column) instanceof JSONArray){
					List<String> column = new ArrayList<String>();
					JSONArray ja = (JSONArray)map.get(MysqlPara.column);
					for(int i=0;i<ja.length();i++){
						column.add(ja.getString(i));
					}
					para.setColumn(column);
				}
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", writeconf, MysqlPara.column, JSONArray.class));
			}
		}else{
			List<String> column = new ArrayList<String>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(MysqlPara.batchSize) && map.get(MysqlPara.batchSize) != null){
			para.setBatchSize(Integer.parseInt(map.get(MysqlPara.batchSize).toString()));
		}
		if(map.containsKey(MysqlPara.writeMode) && map.get(MysqlPara.writeMode) != null){
			para.setWriteMode(map.get(MysqlPara.writeMode).toString());
		}
		if(map.containsKey(MysqlPara.preSql) && map.get(MysqlPara.preSql) != null){
			List<String> preSql = new ArrayList<String>();
			String strs = (String)map.get(MysqlPara.preSql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				preSql.add(pa+";");
			}
			para.setPreSql(preSql);
		}
		if(map.containsKey(MysqlPara.postSql) && map.get(MysqlPara.postSql) != null){
			List<String> postSql = new ArrayList<String>();
			String strs = (String)map.get(MysqlPara.postSql);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				postSql.add(pa+";");
			}
			para.setPostSql(postSql);
		}
		
		if(map.containsKey(MysqlPara.table) && map.get(MysqlPara.table) != null){
			List<String> table = new ArrayList<String>();
			String strs = (String)map.get(MysqlPara.table);
			String[] str = strs.split(";");
			for(String pa : str){
				if(StringUtils.isEmpty(pa) || StringUtils.isBlank(pa)) continue;
				table.add(pa);
			}
			conn.setTable(table);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, MysqlPara.table));
		}
		if(map.containsKey(MysqlPara.jdbcUrl) && map.get(MysqlPara.jdbcUrl) != null){
			conn.setJdbcUrl(map.get(MysqlPara.jdbcUrl).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, MysqlPara.jdbcUrl));
		}
		connection.add(conn);
		para.setConnection(connection);
		
		bean.setParameter(para);
		return bean;
	}
	
}
