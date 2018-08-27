package org.bd.spark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.ReadComm;
import org.bd.spark.enums.FormatType;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 从parquet读取数据<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:26:11 |创建
 */
public class ReadParquetTest {

	public static void readParquet(SparkSession spark) throws Exception{
		Dataset<Row> parquet = ReadComm.getInstance().
				readSource(spark, FormatType.PARQUET, "D:/wpk/devToll/workspace/nkzjProject1/idata-spark/test-file/users.parquet");
		parquet.show();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
