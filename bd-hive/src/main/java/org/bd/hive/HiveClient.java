package org.bd.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.bd.hive.utils.HiveDruidUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> hive-JDBC客户端<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月31日 下午12:01:35 |创建
 */
public class HiveClient implements IHiveClient {

    private Connection conn;
    private Statement stmt;
    private DatabaseMetaData metaData;

    public HiveClient() {
    	this.init();
    }
    
    /**
     * <b>描述：</b> 初始化连接
     * @author wpk | 2017年10月31日 下午2:50:01 |创建
     * @return void
     */
    private void init(){
        try {
			if(conn == null || conn.isClosed()){
	        	conn = HiveDruidUtils.getInstance().getConn();
	            stmt = conn.createStatement();
	            metaData = conn.getMetaData();
			}
		} catch (SQLException e) {
			throw new HiveException(e);
		}
    }

	/**
	 * <b>描述：</b> 执行语句获取结果集
	 * @author wpk | 2017年10月31日 下午12:29:35 |创建
	 * @param hql
	 * @throws IOException
	 * @return void
	 */
	@Override
	public ResultSet executeQuery(String hql) throws Exception {
		return stmt.executeQuery(hql);
	}
    
    /**
     * <b>描述：</b> 执行单条语句
     * @author wpk | 2017年10月31日 下午2:52:11 |创建
     * @return void
     * @throws SQLException 
     */
    @Override
    public int executeUpdate(String hql) throws SQLException {
    	return stmt.executeUpdate(hql);
    }

    /**
     * <b>描述：</b> 执行多条语句
     * @author wpk | 2017年10月31日 下午2:53:53 |创建
     * @return void
     */
    @Override
    public int executeUpdate(String[] hqls) throws SQLException {
    	int count = 0;
    	for (String hql : hqls) {
			if(StringUtils.isBlank(hql)) continue;
			count += stmt.executeUpdate(hql);
		}
    	return count;
    }
    
    /**
     * <b>描述：</b> 获取所有数据库名称
     * @author wpk | 2017年10月31日 下午2:55:39 |创建
     * @param database
     * @throws Exception
     * @return List<String>
     */
    @Override
    public List<String> getHiveDbNames() throws Exception {
        List<String> ret = Lists.newArrayList();
        ResultSet schemas = metaData.getSchemas();
        while (schemas.next()) {
            ret.add(String.valueOf(schemas.getObject(1)));
        }
        DbCloseUtils.close(schemas);
        return ret;
    }

    /**
     * <b>描述：</b> 获取指定数据库下的表
     * @author wpk | 2017年10月31日 下午3:21:12 |创建
     * @param database
     * @throws Exception
     * @return List<String>
     */
    @Override
    public List<String> getHiveTableNames(String database) throws Exception {
        List<String> ret = Lists.newArrayList();
        ResultSet tables = metaData.getTables(null, database, null, null);
        while (tables.next()) {
            ret.add(String.valueOf(tables.getObject(3)));
        }
        DbCloseUtils.close(tables);
        return ret;
    }

    /**
     * <b>描述：</b> 获取指定表的详细信息
     * @author wpk | 2017年10月31日 下午3:31:10 |创建
     * @param database
     * @param tableName
     * @throws Exception
     * @return HiveTableMeta
     */
    @Override
    public HiveTableMeta getHiveTableMeta(String database, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(null, database, tableName, null);
        HiveTableMeta htm = new HiveTableMeta();
        htm.setTableName(tableName);

        List<HiveTableMeta.HiveTableColumnMeta> allColumns = Lists.newArrayList();
        while (columns.next()) {
            allColumns.add(new HiveTableMeta.HiveTableColumnMeta(columns.getString(4), columns.getString(6), columns.getString(12)));
        }
        htm.setFieldColumns(allColumns);
        DbCloseUtils.close(columns);
        stmt.execute("use " + database);
        ResultSet resultSet = stmt.executeQuery("describe formatted " + tableName);
        extractHiveTableMeta(resultSet, htm);
        DbCloseUtils.close(resultSet);
        return htm.buildHiveTableMeta();
    }

    /**
     * <b>描述：</b> 获取hive表中的数据量
     * @author wpk | 2017年10月31日 下午4:11:56 |创建
     * @param database
     * @param tableName
     * @throws Exception
     * @return long
     */
    @Override
    public long getHiveTableRows(String database, String tableName) throws Exception {
        ResultSet resultSet = null;
        long count = 0;
        try {
            resultSet = stmt.executeQuery("select count(*) from " + database + "." + tableName);
            if (resultSet.next()) {
                count = resultSet.getLong(1);
            }
        } finally {
            DbCloseUtils.close(resultSet);
        }
        return count;
    }

    /**
     * <b>描述：</b> 抽取hive表的详细信息
     * @author wpk | 2017年10月31日 下午3:24:19 |创建
     * @param resultSet
     * @param htm
     * @throws SQLException
     * @return void
     */
    private void extractHiveTableMeta(ResultSet resultSet, HiveTableMeta htm) throws SQLException {
        while (resultSet.next()) {
            List<HiveTableMeta.HiveTableColumnMeta> partitionColumns = Lists.newArrayList();
            if ("# Partition Information".equals(resultSet.getString(1).trim())) {
                resultSet.next();
                Preconditions.checkArgument("# col_name".equals(resultSet.getString(1).trim()));
                resultSet.next();
                Preconditions.checkArgument("".equals(resultSet.getString(1).trim()));
                while (resultSet.next()) {
                    if ("".equals(resultSet.getString(1).trim())) {
                        break;
                    }
                    partitionColumns.add(new HiveTableMeta.HiveTableColumnMeta(resultSet.getString(1).trim(), resultSet.getString(2).trim(), resultSet.getString(3).trim()));
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
     * @author wpk | 2017年10月31日 下午4:02:39 |创建
     * @param database
     * @throws Exception
     * @return int
     */
	@Override
	public int dropHiveDbName(String database) throws Exception {
        List<String> tables = getHiveTableNames(database);
        for(String table : tables){
        	dropHiveTableName(database, table);//删除数据库下的所有表
        }
        String hql = "DROP DATABASE " + database;
		return executeUpdate(hql);
	}

    /**
     * <b>描述：</b> 删除指定数据库下的表
     * @author wpk | 2017年10月31日 下午4:04:49 |创建
     * @param database
     * @param tableName
     * @throws Exception
     * @return int
     */
	@Override
	public int dropHiveTableName(String database, String tableName) throws Exception {
        String hql = "DROP TABLE " + database + "." + tableName;
		return executeUpdate(hql);
	}

    /**
     * <b>描述：</b> 判断是否存在指定数据库
     * @author wpk | 2017年10月31日 下午4:19:38 |创建
     * @param database
     * @throws Exception
     * @return boolean
     */
	@Override
	public boolean checkHiveDbName(String database) throws Exception {
		ResultSet resultSet = null;
		boolean flag = false;
        try {
    		String hql = "SHOW DATABASES LIKE '" + database + "'";
            resultSet = executeQuery(hql);
            while (resultSet.next()) {
            	flag = true;
			}
        } finally {
            DbCloseUtils.close(resultSet);
        }
		return flag;
	}
	
    /**
     * <b>描述：</b> 判断是否存在指定的表
     * @author wpk | 2017年10月31日 下午4:37:57 |创建
     * @param database
     * @param tableName
     * @throws Exception
     * @return boolean
     */
	@Override
    public boolean checkHiveTableName(String database, String tableName) throws Exception{
		ResultSet resultSet = null;
		boolean flag = false;
        try {
    		String hql = "SHOW TABLES IN " + database + " LIKE '" + tableName + "'";
            resultSet = executeQuery(hql);
            while (resultSet.next()) {
            	flag = true;
			}
        } finally {
            DbCloseUtils.close(resultSet);
        }
		return flag;
    }

    /**
     * <b>描述：</b> 关闭资源
     * @author wpk | 2017年10月31日 下午5:36:13 |创建
     * @throws Exception
     * @return void
     */
    public void close() {
        DbCloseUtils.close(stmt);
        DbCloseUtils.close(conn);
    }

    public static void main(String[] args) throws Exception {
        HiveClient loader = new HiveClient();
        HiveTableMeta hiveTableMeta = loader.getHiveTableMeta("wpk_test", "grade_test");
        System.out.println(hiveTableMeta);
//        int in1 = loader.execute("CREATE DATABASE wpk_test111");
//        int in = loader.execute("CREATE TABLE `wpk_test111.test1`(`id` string,`name` string,`city` string)");
//        int in1 = loader.dropHiveDbName("wpk_test111");
//        int in = loader.dropHiveTableName("wpk_test111", "test1");
//        System.out.println(in1+"-"+in);
        
        List<String> dbs = loader.getHiveDbNames();
        for (int i = 0; i < dbs.size(); i++) {
			System.out.println(dbs.get(i));
		}
//        
//        List<String> tables = loader.getHiveTableNames("wpk_test");
//        for (int i = 0; i < tables.size(); i++) {
//			System.out.println(tables.get(i));
//		}
//
//        boolean bo1 = loader.checkHiveDbName("wpk_test");
//        boolean bo2 = loader.checkHiveTableName("wpk_test", "hive_test");
//        System.out.println(bo1+"\n"+bo2);
//        
//        int in = loader.executeUpdate("insert overwrite table wpk_test.user_test values ('1','嗯哼','男',4),('2','小小春','男',5),('3','泡芙','女',4)");
//        System.out.println(in);
        loader.close();
    }
}
