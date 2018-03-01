package org.bd.spark.read;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.ReadComm;
import org.bd.spark.SessionDrive;
import org.bd.spark.enums.DbmsType;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 从mysql上读取数据<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:27:10 |创建
 */
public class ReadMysqlTest {

	public static Dataset<Row> readMysql() throws Exception{
		SparkConf conf = new SparkConf();
		conf.setAppName("readMysql");
		conf.setMaster("local[2]");
		conf.set("spark.some.config.option", "some-value");
		conf.set("spark.sql.warehouse.dir","file:///");//不加这一句，启动时会报错Error while instantiating 'org.apache.spark.sql.internal.SessionState'
		SparkSession spark = SessionDrive.getInstance().getSparkSession(conf);
        Dataset<Row> jdbcDF = ReadComm.getInstance().readByJDBC(spark, DbmsType.MYSQL, "a_spark_text");
        jdbcDF.show();
		
//        spark.stop();
        return jdbcDF;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
