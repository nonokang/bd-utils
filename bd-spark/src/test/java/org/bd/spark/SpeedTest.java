package org.bd.spark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.utils.JdbcDriveUtil;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 执行速度测试<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月25日上午9:42:47 |创建
 */
public class SpeedTest {

	public static void main(String[] args) {
		SpeedTest s = new SpeedTest();
		try {
			s.sparkSqlSpeed();
//			s.sparkJdbcSpeed();
//			s.impalaJdbcSpeed();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * <b>描述：impala读取速度测试</b>
	 * @author wpk | 2017年7月24日下午4:05:36 |创建
	 * @throws SQLException
	 */
	/*public void impalaJdbcSpeed() throws SQLException{
		long count = 0;
		Connection conn = ImpalaCommonDao.getInstance().getConn();
		long start = System.currentTimeMillis();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select count(1) from syeas.t_gl_voucherassistrecord");
		while(rs.next()){
			System.out.println("impalaJDBC查询总数："+rs.getString(1));
		}
		long end = System.currentTimeMillis();
		count = end-start;
		System.out.println("impala执行时间:"+count+"ms");
	}*/
	
	/**
	 * <b>描述：sparkJDBC读取速度测试</b>
	 * @author wpk | 2017年7月24日下午4:05:51 |创建
	 * @throws SQLException
	 */
	public void sparkJdbcSpeed() throws SQLException{
		long count = 0;
		Connection conn = JdbcDriveUtil.getInstance().getConn();
		long start = System.currentTimeMillis();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select count(1) from syeas.t_gl_voucherassistrecord");
		while(rs.next()){
			System.out.println("sparkJDBC查询总数："+rs.getString(1));
		}
		long end = System.currentTimeMillis();
		count = end-start;
		System.out.println("sparkJDBC执行时间:"+count+"ms");
	}
	
	/**
	 * <b>描述：sparkSQL读取速度测试</b>
	 * @author wpk | 2017年7月24日下午4:06:24 |创建
	 * @throws Exception
	 */
	public void sparkSqlSpeed() throws Exception{
		long count = 0;
		System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
		SparkConf conf = new SparkConf();
		conf.setAppName("SparkPostHive");
		conf.setMaster("local[2]");
		conf.set("support.type", "hive");
		conf.set("spark.sql.warehouse.dir", "./spark-warehouse");
		SparkSession spark = SessionDrive.getInstance().getSparkSession(conf);
		long start = System.currentTimeMillis();
		spark.sql("show databases").show();
//		spark.sql("select count(1) from syeas.t_gl_voucherassistrecord").show();
		long end = System.currentTimeMillis();
		count = end-start;
		System.out.println("sparkSQL执行时间:"+count+"ms");
        spark.stop();
	}

}
