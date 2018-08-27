package org.bd.spark.write;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.SessionDrive;
import org.bd.spark.WriteComm;
import org.bd.spark.enums.DbmsType;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 写入mysql数据库<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:36:42 |创建
 */
public class WriteMysqlTest {

	public static void writeMysql(Dataset<Row> ds) throws Exception{
		SparkConf conf = new SparkConf();
		conf.setAppName("wirteMysql");
		conf.setMaster("local[2]");
		conf.set("spark.some.config.option", "some-value");
		SparkSession spark = SessionDrive.getInstance().getSparkSession(conf);
        Dataset<Row> jdbcDF = WriteComm.getInstance().writeToJDBC(ds, DbmsType.MYSQL, "a_spark_text1");
        jdbcDF.show();
		
        spark.stop();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
}
