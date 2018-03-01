package org.bd.spark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.ReadComm;
import org.bd.spark.enums.FormatType;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 从json读取数据<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:20:56 |创建
 */
public class ReadJsonTest {

	public static void readJson(SparkSession spark) throws Exception{
		Dataset<Row> json = ReadComm.getInstance().
				readSource(spark, FormatType.JSON, "D:/wpk/devToll/workspace/nkzjProject1/idata-spark/test-file/employees.json");
		json.show();
	}
	
	public static void main(String[] arg){
		
	}
}
