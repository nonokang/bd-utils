package org.bd.spark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.bd.spark.utils.JdbcDriveUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> spark基本操作<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月20日下午3:04:00 |创建
 */
public class SqlComm{
	
	private static Logger logger = LoggerFactory.getLogger(SqlComm.class);
	
	private static SqlComm commonDao = null;
	
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	
	public SqlComm(){}
	
	public static SqlComm getInstance(){
		try {
			if(commonDao == null || commonDao.conn == null || commonDao.conn.isClosed()){
				commonDao = new SqlComm();
			}
		} catch (SQLException e) {
			logger.error("实例SparkCommon异常",e.getMessage());
		}
		return commonDao;
	}
	
	/**
	 * <b>描述：获取spark连接</b>
	 * @author wpk | 2017年7月25日下午6:09:44 |创建
	 * @return
	 */
	public Connection getConn(){
		try {
			if(conn == null || conn.isClosed()){
				conn = JdbcDriveUtil.getInstance().getConn();
			}
		} catch (SQLException e) {
			logger.error("获取spark连接异常:{0}",e.getMessage());
		}
		return conn;
	}
	
	 /**
	  * <b>描述：关闭jdbc连接</b>
	  * @author wpk | 2017年7月25日下午6:10:12 |创建
	  */
    public void close(){
		try {
			if(rs!=null){
				rs.close();
			}
		} catch (Exception e2) {
			logger.error("ResultSet.close()异常:{0}",e2.getMessage());
		}
		try {
			if(stmt!=null){
				stmt.close();
			}
		} catch (Exception e2) {
			logger.error("Statement.close()异常:{0}",e2.getMessage());
		}
		try {
			if(conn!=null){
				conn.close();
			}
		} catch (Exception e2) {
			logger.error("Connection.close()异常:{0}",e2.getMessage());
		}
    }
	
	/**
	 * 根据sql语句查询结果集合
	 * @param sql
	 * @return ResultSet 
	 * @throws SQLException
	 */
    public ResultSet queryData(String sql) throws SQLException {
		stmt = getConn().createStatement();
		rs = stmt.executeQuery(sql);
        return rs;
    }
    
	/**
	 * 执行DDL语句
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
    public int executeSQL(CharSequence sql) throws SQLException {
		stmt = getConn().createStatement();
		return stmt.executeUpdate(sql.toString());
    }

   
    
   /**
    * 根据表名统计全表数据量
    * @param dbName
    * @param tableName
    * @return
    * @throws SQLException
    */
    public int queryCountByTb(String dbName, String tableName) throws SQLException {
    	try {
			String sql = "select count(*) from "+dbName+"."+tableName;
			rs = queryData(sql);  
			ResultSetMetaData metaData= rs.getMetaData();
			while(rs.next()){
				String colname = metaData.getColumnName(1);
				int count = rs.getInt(colname);
				return count;
			}
		} catch (Exception e) {
			logger.error("根据表名{0}统计全表数据量,异常:{1}",tableName,e.getMessage());
			throw new SQLException("根据表名"+dbName+"."+tableName+"统计全表数据量异常!");
		}finally{
			close();
		}
		return 0;
    }
    
    
    /**
     * 根据sql统计数据量
     * @param sql 例如：select count(1) from tableA
     * @return
     * @throws SQLException
     */
    public int queryCountBySql(String sql) throws SQLException {
    	try {
			rs = queryData(sql);  
			ResultSetMetaData metaData= rs.getMetaData();
			while(rs.next()){
				String colname = metaData.getColumnName(1);
				int count = rs.getInt(colname);
				return count;
			}
		} catch (Exception e) {
			logger.error("根据sql={0};统计全表数据量,异常:{1}",sql,e.getMessage());
			throw new SQLException("根据sql="+sql+";统计数据量异常!");
		}finally{
			close();
		}
		return 0;
    }

    /**
     * 检查表是否存在
     * @param tableName tableA 或者 database.tableA
     * @return
     * @throws SQLException
     */
    public boolean checkTable(String tableName) throws SQLException {
    	try {
    		if(StringUtils.isEmpty(tableName))
    			return false;
    		tableName = tableName.toLowerCase();
			String sql = "show tables";
			if(tableName.contains(".")){
				String[] sp = tableName.split("\\.");
				tableName = sp[1];
				sql += " in " + sp[0];
			}
			sql += " like '" + tableName + "'";
			rs = queryData(sql);  
			if(rs.next() && tableName.equals(rs.getString(1)))
				return true;
		} catch (Exception e) {
			logger.error("根据表名={0},检查是否存在,异常:{1}",tableName,e.getMessage());
			throw new SQLException("根据表名="+tableName+",检查是否存在异常!");
		}finally{
			close();
		}
		return false;
    }
    
    /**
     * 判断Impala表字段是否存在
     * @param tableName 表名
     * @param column  列名
     * @return
     * @throws SQLException
     */
    public boolean checkColumn(String tableName, String column) throws SQLException{
    	if(StringUtils.isEmpty(tableName) || StringUtils.isEmpty(column))
    		return false;
    	if(getFields(tableName).contains(column.toLowerCase().trim()))
    		return true;
		return false;
    }
    
    
    /**
     * 获取impala库名列表
     * @return
     * @throws SQLException 
     */
    public List<String> getDataBases() throws SQLException {
    	List<String> list = new ArrayList<String>();
    	String sql = "show databases";
    	try {
			rs = queryData(sql);
			while(rs.next()){
				list.add(rs.getString(1));
			}
		} catch (SQLException e) {
			logger.error("获取impala库名列表异常!");
			throw new SQLException("获取impala库名列表异常!");
		}
    	return list;
    }
    
    /**
     * 获取Impala数据库下全部表名
     * @param databaseName 库名称 如果查默认库的，则传null 或者"" 值
     * @return
     * @throws SQLException
     */
    public List<String> getTables(String databaseName) throws SQLException {
    	List<String> list = new ArrayList<String>();
    	try {
			String sql = "show tables";
			if(StringUtils.isNotEmpty(databaseName) && StringUtils.isNotBlank(databaseName)){
				sql += " in " + databaseName;
			}
			rs = queryData(sql);  
			while(rs.next()){
				list.add(rs.getString(1));
			}
		} catch (Exception e) {
			logger.error("根据库名={0}获取Impala数据库下全部表名异常:{1}",databaseName,e.getMessage());
			throw new SQLException("根据库名="+databaseName+",获取Impala数据库下全部表名异常");
		}finally{
			close();
		}
		return list;
    }
    
    /**
     * 查询该库下有多少张表
     * @param databaseName 库名
     * @return
     * @throws SQLException 
     */
    public int getTableCountByDataBase(String databaseName) throws SQLException {
    	int count = 0;
    	try {
			List<String> list = getTables(databaseName);
			if (list != null && list.size() > 0) {
				count = list.size();
			}
		} catch (SQLException e) {
			logger.error("根据库名={0}查询该库下有多少张表异常:{1}",databaseName,e.getMessage());
			throw new SQLException("根据库名="+databaseName+",查询该库下有多少张表异常");
		}
    	return count;
    }
    
    /**
     * 根据表名获取全部字段名 
     * @param tableName  表名 （例如：tableA 或者 database.tableA）
     * @return
     * @throws SQLException
     */
    public List<String> getFields(String tableName) throws SQLException {
    	List<String> list = new ArrayList<String>();
    	try {
    		if(StringUtils.isEmpty(tableName))
    			return list;
    		rs = queryData("describe " + tableName);//不能使用简写的desc
			while(rs.next()){
				list.add(rs.getString(1));
			}
		} catch (Exception e) {
			logger.error("根据表名＝{0}获取全部字段名异常:{1}",tableName,e.getMessage());
			throw new SQLException("根据表名＝"+tableName+",获取全部字段名异常");
		}finally{
			close();
		}
		return list;
    }
    
    /**
     * 判断表分区是否存在
     * @param tableName  表名
     * @param dbName  库名
     * @param ptnName   分区名
     * @return
     * @throws SQLException
     */
    public boolean checkPartition(String dbName, String tableName, String ptnName) throws SQLException{
    	try {
    		if(StringUtils.isEmpty(tableName) || StringUtils.isEmpty(ptnName))
    			return false;
			StringBuilder tableNameStr = new StringBuilder();
			if (StringUtils.isNotEmpty(dbName)) {
				tableNameStr.append(dbName.toLowerCase()).append(".");
			}
			tableNameStr.append(tableName.toLowerCase());
    		
			String sql = "show partitions " + tableNameStr.toString();
			rs = queryData(sql);
			while(rs.next()){
				if(ptnName.equals(rs.getString(1))){
					return true;
				}
			}
		} catch (Exception e) {
			logger.error("判断表分区(库名＝{0},表名＝{1},分区＝{2})是否存在,异常:{3}",dbName,tableName,ptnName,e.getMessage());
			throw new SQLException("判断表分区(库名＝"+dbName+",表名＝tableName,分区＝ptnName)是否存在,异常");
		}finally{
			close();
		}
		return false;
    }
    
    /**
     * 检查分区字段是否存在    
     * @param tableName
     * @param ptnName
     * @return
     * @throws SQLException
     */
    public boolean checkPartition(String tableName, String ptnName) throws SQLException{
    	return checkPartition(null, tableName, ptnName);
    }

	/**
	 * 给某个表创建分区，若分区已存在，返回true
	 * @param tableName  表名
	 * @param dbName     库名
	 * @param ptnName    分区字段名
	 * @param ptnValue   分区字段值
	 * @return true: 成功
	 *         false：失败
	 * @throws SQLException 若表不是分区表，抛出异常
	 * @author
	 */
	public boolean addPartition(String dbName, String tableName, String ptnName, String ptnValue) throws SQLException {
		if(StringUtils.isEmpty(tableName)){
			throw new SQLException("table name is null!");
		}
		if(StringUtils.isEmpty(ptnName)){
			throw new SQLException("name of table partition is null!");
		}
		
		//若分区已存在，返回true
		if(checkPartition(dbName, tableName, ptnValue)){
			logger.info("partition " + ptnValue + " of table " + tableName + " is exist..");
			return true;
		}
		StringBuilder sql = new StringBuilder("alter table ");
		if (StringUtils.isNotEmpty(dbName)) {
			sql.append(dbName).append(".");
		}
		sql.append(tableName).append(" add partition (").append(ptnName)
				.append("='").append(ptnValue).append("')");
		logger.info("start add table partition,sql = " + sql.toString());
		try{
			executeSQL(sql.toString());
		}catch(SQLException e){
			throw e;
		}
		logger.info("add table partition succeed,sql = " + sql.toString());
		return true;
	}
    
	/**
	 * 添加分区字段
	 * @param tableName  表名
	 * @param ptnName 分区字段名
	 * @param ptnValue 分区字段值
	 * @return  true: 成功
	 *          false：失败
	 * @throws SQLException
	 */
	public boolean addPartition(String tableName, String ptnName, String ptnValue) throws SQLException {
		return addPartition(null, tableName, ptnName, ptnValue);
	}
	
	/**
	 * 刪除表分区，若分区不存在，返回true
	 * @param tableName  表名
	 * @param dbName     库名
	 * @param ptnName    分区字段名
	 * @param ptnValue   分区字段值
	 * @return true: 成功
	 *         false：失败
	 * @throws SQLException 若表不是分区表，抛出异常
	 * @author 
	 */
	public boolean dropPartition(String dbName, String tableName, String ptnName, String ptnValue) throws SQLException {
		if (StringUtils.isEmpty(tableName)) {
			throw new SQLException("table name is null!");
		}
		if (StringUtils.isEmpty(ptnName)) {
			throw new SQLException("name of table partition is null!");
		}
		// 若分区不存在，返回true
		if (checkPartition(dbName, tableName, ptnValue)) {
			logger.info("partition " + ptnValue + " of table " + tableName
					+ " not exist..");
			return true;
		}
		StringBuilder sql = new StringBuilder("alter table ");
		if (StringUtils.isNotEmpty(dbName)) {
			sql.append(dbName).append(".");
		}
		sql.append(tableName).append(" drop partition (").append(ptnName)
				.append("='").append(ptnValue).append("')");
		logger.info("start drop table partition,sql = " + sql.toString());
		try {
			executeSQL(sql.toString());
		} catch (SQLException e) {
			throw e;
		}
		logger.info("drop table partition succeed,sql = " + sql.toString());
		return true;
	}
	
}
