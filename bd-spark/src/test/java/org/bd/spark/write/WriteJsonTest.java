package org.bd.spark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bd.spark.WriteComm;
import org.bd.spark.enums.FormatType;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 写入json文件<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:38:03 |创建
 */
public class WriteJsonTest {

	public static void writeJson(Dataset<Row> ds) throws Exception{
		Dataset<Row> parquet = WriteComm.getInstance().writeToFile(ds, FormatType.JSON, "C:/Users/Administrator/Desktop/sparkFile/role.json");
		parquet.show();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
