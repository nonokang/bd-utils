package org.bd.impala;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.bd.impala.utils.ImpaIaTableColumn;
import org.bd.impala.utils.ImpalaConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>版权信息:</b> 2017,广州智数信息科技有限公司<br/>
 * <b>功能描述:</b> Impala的基本操作<br/>
 * <b>版本历史:</b><br/>
 * @author  jasse | 2017年3月7日|创建
 */
@Deprecated
public class ImpalaCommonDao{
	
	private static Logger logger = LoggerFactory.getLogger(ImpalaCommonDao.class);
	
	private static ImpalaCommonDao commonDao = null;
	
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	
	public ImpalaCommonDao(){
	}
	
	public static ImpalaCommonDao getInstance(){
		try {
			if(commonDao == null || commonDao.conn == null || commonDao.conn.isClosed()){
				commonDao = new ImpalaCommonDao();
			}
		} catch (SQLException e) {
			logger.error("实例ImpalaCommonDao异常",e.getMessage());
		}
		return commonDao;
	}
	
	/**获取Impala连接*/
	public Connection getConn(){
		try {
			if(conn == null || conn.isClosed()){
				conn = ImpalaConnection.getInstance().getConn();
			}
		} catch (SQLException e) {
			logger.error("获取impala连接异常:{0}",e.getMessage());
		}
		return conn;
	}
	
	 /** 关闭impala连接*/
    public void close(){
		try {rs.close();} catch (Exception e2) {
			logger.error("ResultSet.close()异常:{0}",e2.getMessage());
		}
		try {stmt.close();} catch (Exception e2) {
			logger.error("Statement.close()异常:{0}",e2.getMessage());
		}
		try {conn.close();} catch (Exception e2) {
			logger.error("Connection.close()异常:{0}",e2.getMessage());
		}
    }
	
	/**
	 * impala表文件格式的枚举
	 */
	public enum TabFileFormat {PARQUET,TEXTFILE,AVRO,SEQUENCEFILE,RCFILE};
	
	
	
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
    * @param tableName
    * @return
    * @throws SQLException
    */
    public int queryCount(String tableName) throws SQLException {
    	try {
			String sql = "select count(*) from "+tableName;
			rs = queryData(sql);  
			ResultSetMetaData metaData= rs.getMetaData();
			while(rs.next()){
				String colname = metaData.getColumnName(1);
				int count = rs.getInt(colname);
				return count;
			}
		} catch (Exception e) {
			logger.error("根据表名{0}统计全表数据量,异常:{1}",tableName,e.getMessage());
			throw new SQLException("根据表名"+tableName+"统计全表数据量异常!");
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
    		tableName = tableName.toLowerCase();//Impala默认表名及字段名小写
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
    	if(getFields(tableName).contains(column.toLowerCase().trim()))//Impala默认表名及字段名小写
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
     * @param databaseName  databse名
     * @param ptnName   分区名
     * @return
     * @throws SQLException
     */
    public boolean checkPartition(String tableName, String databaseName, String ptnName) throws SQLException{
    	try {
    		if(StringUtils.isEmpty(tableName) || StringUtils.isEmpty(ptnName))
    			return false;
			StringBuilder tableNameStr = new StringBuilder();
			if (StringUtils.isNotEmpty(databaseName)) {
				tableNameStr.append(databaseName.toLowerCase()).append(".");
			}
			tableNameStr.append(tableName.toLowerCase());//Impala默认表名及字段名小写
    		
			String sql = "show partitions " + tableNameStr.toString();
			rs = queryData(sql);
			while(rs.next()){
				if(ptnName.equals(rs.getString(1))){
					return true;
				}
			}
		} catch (Exception e) {
			logger.error("判断表分区(库名＝{0},表名＝{1},分区＝{2})是否存在,异常:{3}",databaseName,tableName,ptnName,e.getMessage());
			throw new SQLException("判断表分区(库名＝"+databaseName+",表名＝tableName,分区＝ptnName)是否存在,异常");
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
    	return checkPartition(tableName, null, ptnName);
    }

    
    /**
     * 获取Impala数据库下表的结构信息，包括“name”、“type”、“comment”信息。
     * 若表不存在，在返回长度为0的list
     * @param tableName 要获取结构的表名
     * @param dbName    要获取结构的表所在的database名, 若为null，则从
     * @return  List<ImpalaTableColumn>;，其中每个ImpalaTableColumn包含三个属性的值，分别是"name"、"type"、"comment" 
     * @throws SQLException
     * @author
     */
	public List<ImpaIaTableColumn> getTableColumns(String tableName,
			String dbName) throws SQLException {
		List<ImpaIaTableColumn> mapList = new ArrayList<ImpaIaTableColumn>();
		try {
			if (StringUtils.isEmpty(tableName)) {
				return mapList;
			}
			//判断表是否存在，若不存在，返回
			if(!checkTable(StringUtils.isNotEmpty(dbName) ? dbName+"."+tableName : tableName)){
				return mapList;
			}
			rs = queryData("describe "
					+ (StringUtils.isEmpty(dbName) ? "" : dbName
							+ ".") + tableName);
			while (rs.next()) {
				ImpaIaTableColumn column = new ImpaIaTableColumn();
				column.setName(rs.getString(ImpaIaTableColumn.COLUMN_NAME));
				column.setType(rs.getString(ImpaIaTableColumn.COLUMN_TYPE));
				column.setComment(rs
						.getString(ImpaIaTableColumn.COLUMN_COMMENT));
				mapList.add(column);
			}
		} catch (SQLException e) {
			logger.error("Get the Structure of table " + tableName
					+ " in database " + " dbName error! errmsg:"
					+ e.getMessage());
			throw e;
		} finally {
			close();
		}
		logger.info("Get the Structure of table " + tableName + " in database "
				+ " dbName success..");
		return mapList;
	}
	
	/**
	 * 根据表名获取字段名称
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public List<ImpaIaTableColumn> getTableColumns(String tableName)
			throws SQLException {
		return getTableColumns(tableName, null);
	}
	
	
	/**
	 * 在Impala创建一张表，若表已存在，抛出异常
	 * @param tableName  要创建的表名，为空则抛出异常
	 * @param dbName     要创建的表所在的database，为空则在默认的database创建表
	 * @param columns    列对象（DataBaseTableColumn）列表，为空则抛出异常
	 * @param ptnColumn  分区列对象，为空则创建堆表（非分区表）
	 * @param fieldsTerminated  列分隔符，可为空
	 * @param linesTerminated   行分隔符，可为空
	 * @param file_format  文件格式，可为空，默认为文本
	 * @return true: 删除成功<br/>
	 *         false：删除失败
	 * @throws SQLException
	 * @author 
	 */
	public boolean creatTable(String tableName, String dbName,
			List<ImpaIaTableColumn> columns, ImpaIaTableColumn ptnColumn,
			Character fieldsTerminated, Character linesTerminated,
			TabFileFormat file_format) throws SQLException {
		if (StringUtils.isEmpty(tableName)) { // 表名为空，抛出Exception
			throw new SQLException("tableName is null..");
		}
		if (columns == null || columns.size() == 0) { // 字段列表为空，抛出Exception
			throw new SQLException("columns is null..");
		}
		
		//判断表是否存在，若存在返回false
		String tableStr = StringUtils.isNotEmpty(dbName) ? dbName+"."+tableName : tableName;
		if(checkTable(tableStr)){
			throw new SQLException("table " + tableStr + " exists!");
		}

		StringBuilder sql = new StringBuilder("create table ");
		// 配置dbName
		if (StringUtils.isNotEmpty(dbName)) {
			sql.append(dbName).append(".");
		}
		// 配置表名
		sql.append(tableName);
		// 配置列
		StringBuilder columnStr = new StringBuilder(" (");
		Iterator<ImpaIaTableColumn> iterator = columns.iterator();
		ImpaIaTableColumn first = iterator.next();
		if (first != null) {
			columnStr.append(first.getName()); // 字段名
			columnStr.append(" ").append(first.getType());// 字段类型
			if (StringUtils.isNotEmpty(first.getComment())) { // 字段说明
				columnStr.append(" COMMENT '").append(first.getComment())
						.append("'");
			}
		}
		
		while (iterator.hasNext()) {
			ImpaIaTableColumn column = iterator.next();
			columnStr.append(", ").append(column.getName()); // 字段名
			columnStr.append(" ").append(column.getType()); // 字段类型
			if (StringUtils.isNotEmpty(column.getComment())) { // 字段说明
				columnStr.append(" COMMENT '").append(column.getComment())
						.append("'");
			}
		}
		columnStr.append(") ");
		sql.append(columnStr);

		// 配置分区
		if (ptnColumn != null && StringUtils.isNotEmpty(ptnColumn.getName())) {
			sql.append("partitioned by(").append(ptnColumn.getName())
					.append(" ").append(ptnColumn.getType()).append(")");
		}

		// 配置列行分隔符
		if (fieldsTerminated != null || linesTerminated != null) {
			sql.append("row format delimited");
			if (fieldsTerminated != null) {
				sql.append(" fields terminated by '").append(fieldsTerminated)
						.append("' ");
			}
			if (linesTerminated != null) {
				sql.append(" lines terminated by '").append(linesTerminated)
						.append("' ");
			}
		}
		// 配置文件格式,若输入参数中为空，则默认文本格式
		if (file_format != null) {
			sql.append("stored as ").append(file_format.toString());
		} else {
			sql.append("stored as ").append(TabFileFormat.TEXTFILE.toString());
		}

		// 执行建表语句
		logger.info("start create table,sql = " + sql.toString());
		try{
			executeSQL(sql.toString());
		}catch(SQLException e){
			throw e;
		}
		logger.info("create table success, sql=" + sql.toString());
		return true;
	}
    
	/**
	 * 创建表
	 * @param tableName  表
	 * @param columns   列字段
	 * @param ptnColumn 分区字段
	 * @param fieldsTerminated  字段分隔符
	 * @param linesTerminated    行分隔符
	 * @param file_format  store方式
	 * @return
	 * @throws SQLException
	 */
	public boolean creatTable(String tableName, List<ImpaIaTableColumn> columns,
			ImpaIaTableColumn ptnColumn, Character fieldsTerminated,
			Character linesTerminated, TabFileFormat file_format)
			throws SQLException {
		return creatTable(tableName, null, columns, ptnColumn, fieldsTerminated,
				linesTerminated, file_format);
	}

	/**
	 * 创建表
	 * @param tableName
	 * @param columns
	 * @param ptnColumn
	 * @param file_format
	 * @return
	 * @throws SQLException
	 */
	public boolean creatTable(String tableName, List<ImpaIaTableColumn> columns,
			ImpaIaTableColumn ptnColumn, TabFileFormat file_format)
			throws SQLException {
		return creatTable(tableName, null, columns, ptnColumn, null, null, file_format);
	}
	
	
	/**
	 * 给某个表创建分区，若分区已存在，返回true
	 * @param tableName  表名
	 * @param dbName     database名称
	 * @param ptnName    分区字段名
	 * @param ptnValue   分区字段值
	 * @return true: 成功<br/>
	 *         false：失败
	 * @throws SQLException 若表不是分区表，抛出异常
	 * @author
	 */
	public boolean addPartition(String tableName, String dbName, String ptnName,
			String ptnValue) throws SQLException {
		if(StringUtils.isEmpty(tableName)){
			throw new SQLException("table name is null!");
		}
		if(StringUtils.isEmpty(ptnName)){
			throw new SQLException("name of table partition is null!");
		}
		
		//若分区已存在，返回true
		if(checkPartition(tableName, dbName, ptnValue)){
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
	 * @return  true: 成功<br/>
	 *          false：失败
	 * @throws SQLException
	 */
	public boolean addPartition(String tableName, String ptnName, String ptnValue)
			throws SQLException {
		return addPartition(tableName, null, ptnName, ptnValue);
	}
	
	/**
	 * 刪除表分区，若分区不存在，返回true
	 * @param tableName  表名
	 * @param dbName     database名称
	 * @param ptnName    分区字段名
	 * @param ptnValue   分区字段值
	 * @return true: 成功<br/>
	 *         false：失败
	 * @throws SQLException 若表不是分区表，抛出异常
	 * @author 
	 */
	public boolean dropPartition(String tableName, String dbName,
			String ptnName, String ptnValue) throws SQLException {
		if (StringUtils.isEmpty(tableName)) {
			throw new SQLException("table name is null!");
		}
		if (StringUtils.isEmpty(ptnName)) {
			throw new SQLException("name of table partition is null!");
		}
		// 若分区不存在，返回true
		if (checkPartition(tableName, dbName, ptnValue)) {
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
	

	/**
	 * 重命名impala表名
	 * @param database
	 * @param oldName   旧表名
	 * @param newName   新表名
	 * @return
	 * @throws SQLException 若表不存在，抛出SQLException
	 * @author 
	 */
	public boolean reNameTable(String database, String oldName, String newName) throws SQLException{
		if(StringUtils.isEmpty(oldName)){
			throw new SQLException("name of table which will to be renamed is null!");
		}
		if(StringUtils.isEmpty(newName)){
			throw new SQLException("new name to be renamed for table " + oldName + " is null!");
		}
		StringBuilder sql = new StringBuilder("alter table ");
		if(StringUtils.isNotEmpty(database)){
			sql.append(StringUtils.trim(database)).append(".").append(oldName);
			sql.append(" rename to ");
			sql.append(StringUtils.trim(database)).append(".").append(newName);
		}else{
			sql.append(oldName).append(" rename to ").append(newName);
		}
		logger.info("start rename table " + oldName + ", sql = " + sql.toString());
		try{
			executeSQL(sql.toString());
		}catch(SQLException e){
			logger.info("rename table " + oldName + " error.", e.getMessage());
			throw new SQLException("rename table " + oldName + " error." , e);
		}
		return true;
	}

	
	/**
	 * 同步impala元数据
	 * @param impalaTableName 可选,指定需要同步的表名
	 * @throws SQLException
	 */
	public void syncImpalaMetadata(String impalaTableName) throws SQLException{
		try {
			String sql = "INVALIDATE METADATA %s";
			if(impalaTableName != null){
				sql = String.format(sql, impalaTableName);
			}
			executeSQL(sql);
		} catch (SQLException e) {
			throw new SQLException("impala同步元数据表失败："+e.getMessage(), e);
		}
	}
    
	
	
	/**
	 * 删除impala视图
	 * @param viewName
	 * @param dbName
	 * @throws SQLException
	 */
	public void dropView(String viewName, String dbName) throws SQLException{
		if (StringUtils.isEmpty(viewName)) {
			logger.error("tableName is null, drop no view..");
			return;
		}
		String sql = "drop view if exists %s%s";
		dbName = dbName == null || dbName.isEmpty() ? "" : dbName+".";
		sql = String.format(sql, dbName, viewName);
		try {
			executeSQL(sql);
		} catch (SQLException e) {
			logger.error("drop view exception", e);
			throw e;
		}
	}
	
	/**
	 * <b>描述：</b> 刷新元数据
	 * @author wpk | 2017年11月2日 下午2:30:04 |创建
	 * @throws SQLException
	 * @return void
	 */
	public void refreshMetadata() throws SQLException{
		executeSQL("invalidate metadata");
	}
}
