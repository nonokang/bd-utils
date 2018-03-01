package org.bd.spark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.ReadComm;
import org.bd.spark.enums.FormatType;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 从csv读取数据<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:22:57 |创建
 */
public class ReadCsvTest {

	public static void readCsv(SparkSession spark) throws Exception{
	    /*CSVWriter writer =new CSVWriter(new FileWriter("D:/wpk/devToll/workspace/nkzjProject1/idata-spark/test-file/people.csv"),',');
	    CSVReader reader =new CSVReader(new Reader(),'\t');*/
		
		Dataset<Row> csv = ReadComm.getInstance().
				readSource(spark, FormatType.CSV, "D:/wpk/devToll/workspace/nkzjProject1/idata-spark/test-file/people.csv");
		csv.show();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
