package org.bd.impala;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.bd.impala.utils.ImpalaDruidUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> impala客户端<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月11日 下午8:59:42 |创建
 */
public class ImpalaClient {

    private Connection conn;
    private Statement stmt;
	private ResultSet rs;
    private DatabaseMetaData metaData;
	
	public ImpalaClient(){
		init();
	}

	/**
	 * <b>描述：</b> 初始化连接
	 * @author wpk | 2017年12月11日 下午9:03:43 |创建
	 * @return void
	 */
    private void init(){
        try {
			if(conn == null || conn.isClosed()){
	        	conn = ImpalaDruidUtils.getInstance().getConn();
	            stmt = conn.createStatement();
	            metaData = conn.getMetaData();
			}
		} catch (SQLException e) {
			throw new ImpalaException(e);
		}
    }
    
    /**
     * <b>描述：</b> 获取连接对象
     * @author wpk | 2017年12月11日 下午9:06:50 |创建
     * @return Connection
     */
    public Connection getConn(){
    	if(null == conn){
    		init();
    	}
    	return conn;
    }
    
    public void closeRs(){
    	if(null != rs){
        	try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }
    
    public void closeStmt(){
    	if(null != stmt){
    		try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }
    
    public void closeConn(){
    	if(null != conn){
    		try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }
    
    public void close(){
    	closeRs();
    	closeStmt();
    	closeConn();
    }
    
    /**
     * <b>描述：</b> 查询语句获取结果集
     * @author wpk | 2017年12月11日 下午9:14:05 |创建
     * @param sql
     * @throws Exception
     * @return ResultSet
     */
	public ResultSet executeQuery(String sql) throws Exception {
		return stmt.executeQuery(sql);
	}
	
	/**
	 * <b>描述：</b> 执行语句
	 * @author wpk | 2017年12月12日 下午9:18:11 |创建
	 * @param sql
	 * @throws Exception
	 * @return int
	 */
	public int executeUpdate(String sql) throws Exception {
		int count = stmt.executeUpdate(sql);
		return count;
	}
	
	/**
	 * <b>描述：</b> 执行语句
	 * @author wpk | 2017年12月12日 下午9:30:29 |创建
	 * @param sqls
	 * @throws Exception
	 * @return int
	 */
	public int executeUpdate(String[] sqls) throws Exception {
		int count = 0;
		for(String sql : sqls){
			if(StringUtils.isBlank(sql)) continue;
			count += executeUpdate(sql);
		}
		return count;
	}
	
	/**
	 * <b>描述：</b> 刷新元数据
	 * @author wpk | 2017年12月12日 下午9:32:22 |创建
	 * @throws Exception
	 * @return void
	 */
	public void syncMetadata() throws Exception{
		executeUpdate("invalidate metadata");
	}
	
	/**
	 * <b>描述：</b> 获取数据库名称
	 * @author wpk | 2017年12月12日 下午9:42:56 |创建
	 * @throws Exception
	 * @return List<String>
	 */
    public List<String> getDbNames() throws Exception {
        List<String> ret = Lists.newArrayList();
        rs = metaData.getSchemas();
        while (rs.next()) {
            ret.add(String.valueOf(rs.getObject(1)));
        }
        closeRs();
        return ret;
    }
    
    /**
     * <b>描述：</b> 通过指定数据库名获取所有表名
     * @author wpk | 2017年12月12日 下午9:43:50 |创建
     * @param database
     * @throws Exception
     * @return List<String>
     */
    public List<String> getTableNamesByDb(String database) throws Exception {
        List<String> ret = Lists.newArrayList();
        rs = metaData.getTables(null, database, null, null);
        while (rs.next()) {
            ret.add(String.valueOf(rs.getObject(3)));
        }
        closeRs();
        return ret;
    }
    
    /**
     * <b>描述：</b> 获取表数据总数
     * @author wpk | 2017年12月12日 下午9:55:56 |创建
     * @param database
     * @param tableName
     * @throws Exception
     * @return long
     */
    public long getTableRows(String database, String tableName) throws Exception {
        long count = 0;
        try {
            rs = stmt.executeQuery("select count(*) from " + database + "." + tableName);
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } finally {
            closeRs();
        }
        return count;
    }

    /**
     * <b>描述：</b> 通过指定表名获取列名
     * @author wpk | 2017年12月12日 下午9:59:56 |创建
     * @param tableName
     * @throws Exception
     * @return long
     */
    public List<String> getColumnsByTb(String tableName) throws Exception {
    	List<String> list = new ArrayList<String>();
    	try {
    		if(StringUtils.isBlank(tableName)) return list;
    		rs = executeQuery("describe " + tableName);//不能使用简写的desc
			while(rs.next()){
				list.add(rs.getString(1));
			}
		} finally{
			closeRs();
		}
		return list;
    }
    
    /**
     * <b>描述：</b> 获取指定表详细信息
     * @author wpk | 2017年12月12日 下午10:04:19 |创建
     * @param database
     * @param tableName
     * @throws SQLException
     * @return ImpalaTableMeta
     */
    public ImpalaTableMeta getHiveTableMeta(String database, String tableName) throws SQLException {
        rs = metaData.getColumns(null, database, tableName, null);
        ImpalaTableMeta htm = new ImpalaTableMeta();
        htm.setTableName(tableName);

        List<ImpalaTableMeta.ImpalaTableColumnMeta> allColumns = Lists.newArrayList();
        while (rs.next()) {
            allColumns.add(new ImpalaTableMeta.ImpalaTableColumnMeta(rs.getString(4), rs.getString(6), rs.getString(12)));
        }
        htm.setFieldColumns(allColumns);
        closeRs();
        stmt.execute("use " + database);
        rs = stmt.executeQuery("describe formatted " + tableName);
        extractTableMeta(rs, htm);
        closeRs();
        return htm.buildImpalaTableMeta();
    }
    
    private void extractTableMeta(ResultSet resultSet, ImpalaTableMeta htm) throws SQLException {
        while (resultSet.next()) {
            List<ImpalaTableMeta.ImpalaTableColumnMeta> partitionColumns = Lists.newArrayList();
            if ("# Partition Information".equals(resultSet.getString(1).trim())) {
                resultSet.next();
                Preconditions.checkArgument("# col_name".equals(resultSet.getString(1).trim()));
                resultSet.next();
                Preconditions.checkArgument("".equals(resultSet.getString(1).trim()));
                while (resultSet.next()) {
                    if ("".equals(resultSet.getString(1).trim())) {
                        break;
                    }
                    partitionColumns.add(new ImpalaTableMeta.ImpalaTableColumnMeta(resultSet.getString(1).trim(), resultSet.getString(2).trim(), resultSet.getString(3).trim()));
                }
                htm.setPartitionColumns(partitionColumns);
            }

            if ("Owner:".equals(resultSet.getString(1).trim())) {
            	htm.setOwner(resultSet.getString(2).trim());
            }
            if ("LastAccessTime:".equals(resultSet.getString(1).trim())) {
                try {
                    int i = Integer.parseInt(resultSet.getString(2).trim());
                    htm.setLastAccessTime(i);
                } catch (NumberFormatException e) {
                	htm.setLastAccessTime(0);
                }
            }
            if ("Location:".equals(resultSet.getString(1).trim())) {
            	htm.setLocation(resultSet.getString(2).trim());
            }
            if ("Table Type:".equals(resultSet.getString(1).trim())) {
            	htm.setTableType(resultSet.getString(2).trim());
            }
            if ("Table Parameters:".equals(resultSet.getString(1).trim())) {
                while (resultSet.next()) {
                    if (resultSet.getString(2) == null) {
                        break;
                    }
                    if ("storage_handler".equals(resultSet.getString(2).trim())) {
                        htm.setNative(false);//default is true
                    }
                    if ("totalSize".equals(resultSet.getString(2).trim())) {
                    	htm.setFileSize(Long.parseLong(resultSet.getString(3).trim()));
                    }
                    if ("numFiles".equals(resultSet.getString(2).trim())) {
                    	htm.setFileNum(Long.parseLong(resultSet.getString(3).trim()));
                    }
                    if ("skip.header.line.count".equals(resultSet.getString(2).trim())) {
                      if (null == resultSet.getString(3).trim())
                          htm.setSkipHeaderLineCount(0);
                      else
                    	  htm.setSkipHeaderLineCount(Integer.parseInt(resultSet.getString(3).trim()));
                    }
                }
            }
            if ("InputFormat:".equals(resultSet.getString(1).trim())) {
            	htm.setInputFormat(resultSet.getString(2).trim());
            }
            if ("OutputFormat:".equals(resultSet.getString(1).trim())) {
            	htm.setOutputFormat(resultSet.getString(2).trim());
            }
        }
    }
    
    /**
     * <b>描述：</b> 删除数据库（注意：删除数据库将同时删除库中所有表）
     * @author wpk | 2017年12月12日 下午10:08:35 |创建
     * @param database
     * @throws Exception
     * @return int
     */
	public int dropDbName(String database) throws Exception {
        List<String> tables = getTableNamesByDb(database);
        for(String table : tables){
        	dropTableName(database, table);//删除数据库下的所有表
        }
        String hql = "DROP DATABASE " + database;
		return executeUpdate(hql);
	}
	
	/**
	 * <b>描述：</b> 删除指定数据库下的表
	 * @author wpk | 2017年12月12日 下午10:09:09 |创建
	 * @param database
	 * @param tableName
	 * @throws Exception
	 * @return int
	 */
	public int dropTableName(String database, String tableName) throws Exception {
        String hql = "DROP TABLE " + database + "." + tableName;
		return executeUpdate(hql);
	}

	/**
	 * <b>描述：</b> 判断是否存在指定数据库
	 * @author wpk | 2017年12月12日 下午10:10:11 |创建
	 * @param database
	 * @throws Exception
	 * @return boolean
	 */
	public boolean checkDbName(String database) throws Exception {
		boolean flag = false;
        try {
    		String hql = "SHOW DATABASES LIKE '" + database + "'";
            rs = executeQuery(hql);
            while (rs.next()) {
            	flag = true;
			}
        } finally {
            closeRs();
        }
		return flag;
	}

	/**
	 * <b>描述：</b> 判断是否存在指定的表
	 * @author wpk | 2017年12月12日 下午10:11:07 |创建
	 * @param database
	 * @param tableName
	 * @throws Exception
	 * @return boolean
	 */
    public boolean checkTableName(String database, String tableName) throws Exception{
		boolean flag = false;
        try {
    		String hql = "SHOW TABLES IN " + database + " LIKE '" + tableName + "'";
            rs = executeQuery(hql);
            while (rs.next()) {
            	flag = true;
			}
        } finally {
            closeRs();
        }
		return flag;
    }
}
