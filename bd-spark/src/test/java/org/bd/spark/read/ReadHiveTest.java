package org.bd.spark.read;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.SessionDrive;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 从hive读取数据<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:28:05 |创建
 */
public class ReadHiveTest {

	public static Dataset<Row> readHive() throws Exception{
		SparkConf conf = new SparkConf();
		conf.setAppName("SparkPostHive");
		conf.setMaster("local[2]");
		conf.set("support.type", "hive");
		conf.set("spark.sql.warehouse.dir", "./spark-warehouse");//不加这一句，启动时会报错Error while instantiating 'org.apache.spark.sql.internal.SessionState'
		SparkSession spark = SessionDrive.getInstance().getSparkSession(conf);
		
		spark.sql("select count(1) from syeas.t_gl_voucherassistrecord").show();

		Dataset<Row> ds1 = spark.sql("select * from syeas.t_gl_voucherassistrecord limit 5");
		ds1.show();
		
        //关闭程序
//        spark.stop();
		
		return ds1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
