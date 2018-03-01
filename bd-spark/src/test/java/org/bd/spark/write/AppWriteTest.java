package org.bd.spark.write;

import java.sql.SQLException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.read.ReadMysqlTest;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 写入不同库、文件测试<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月25日下午3:23:06 |创建
 */
public class AppWriteTest {

	public static void main(String[] args) {
		try {
			System.setProperty("hadoop.home.dir", "F:\\hadoop-common-2.2.0-bin-master");
			Dataset<Row> mysql = ReadMysqlTest.readMysql();//从mysql读取
			
	        //创建临时表用于sql查询
			mysql.createOrReplaceTempView("role");
			SparkSession spark = mysql.sparkSession();
	        Dataset<Row> sqlDF = spark.sql("select id,name,code,operateStatus from role");
	        sqlDF.show();
			
			WriteJsonTest.writeJson(sqlDF);//写入json文件
			WriteCsvTest.writeCsv(sqlDF);//写入csv文件
			WriteTextTest.writeTxt(sqlDF);//写入text文件
			WriteParquetTest.writeParquet(sqlDF);//写入parquet文件
			
			//写入数据库
//			WriteMysqlTest.writeMysql(sqlDF);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
}
