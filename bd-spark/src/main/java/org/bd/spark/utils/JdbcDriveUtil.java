package org.bd.spark.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.bd.spark.enums.Consts;


/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> spark的jdbc驱动类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月20日上午11:45:44 |创建
 */
public class JdbcDriveUtil{
	
	private volatile static JdbcDriveUtil sc = null;
	
	/** 默认构造类*/
	public JdbcDriveUtil() {
		try {
			if(sc == null){
				SysVarsUtils sysVarsUtils = SysVarsUtils.getInstance();
				String driver = sysVarsUtils.getVarByName(Consts.spark_driver);
				Class.forName(driver);
			}
		} catch(ClassNotFoundException e) {
			System.out.println(String.format("spark的jdbc驱动加载异常：(%s)",e.getMessage()));
			e.printStackTrace();
		} 
	}
	
	/**
	 * <b>描述：接口</b>
	 * @author wpk | 2017年7月24日下午4:54:22 |创建
	 * @return
	 */
	public static JdbcDriveUtil getInstance(){
		if(sc == null){
			synchronized (JdbcDriveUtil.class) {
				if(sc == null){
					sc = new JdbcDriveUtil();
				}
			}
		}
		return sc;
	}
	
	/**
	 * <b>描述：获取JDBC连接</b>
	 * @author wpk | 2017年7月24日下午5:00:59 |创建
	 * @return
	 * @throws SQLException
	 */
	public Connection getConn() throws SQLException{
		SysVarsUtils sysVarsUtils = SysVarsUtils.getInstance();
		String url = sysVarsUtils.getVarByName(Consts.spark_url);
		String user = sysVarsUtils.getVarByName(Consts.spark_user);
		String pwd = sysVarsUtils.getVarByName(Consts.spark_password);
		Connection conn = DriverManager.getConnection(url,user,pwd); 
		return conn;
	}
}