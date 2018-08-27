package org.bd.spark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bd.spark.WriteComm;
import org.bd.spark.enums.FormatType;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 写入csv文件<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:39:20 |创建
 */
public class WriteCsvTest {

	public static void writeCsv(Dataset<Row> ds) throws Exception{
		Dataset<Row> parquet = WriteComm.getInstance().writeToFile(ds, FormatType.CSV, "C:/Users/Administrator/Desktop/sparkFile/role.csv");
		parquet.show();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
