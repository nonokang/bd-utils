package org.bd.spark.read;

import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.SessionDrive;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 从不同库、文件读取数据测试<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月25日上午9:43:09 |创建
 */
public class AppReadTest {

	public static void main(String[] args) {
		try {
			System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
			SparkConf conf = new SparkConf();
			conf.setAppName("readTest");
			conf.setMaster("local[2]");
			conf.set("spark.some.config.option", "some-value");
			SparkSession spark = SessionDrive.getInstance().getSparkSession(conf);
			
			ReadJsonTest.readJson(spark);//读取json文件
			ReadCsvTest.readCsv(spark);//读取csv文件
			ReadTextTest.readText(spark);//读取text文件
			ReadParquetTest.readParquet(spark);//读取parquet文件
			spark.stop();
			
			ReadMysqlTest.readMysql();//读取数据库
			
			ReadHiveTest.readHive();//读取hive表
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
}
