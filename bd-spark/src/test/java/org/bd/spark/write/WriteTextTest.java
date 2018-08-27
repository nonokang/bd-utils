package org.bd.spark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bd.spark.WriteComm;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 写入text文件<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年1月16日 上午11:40:55 |创建
 */
public class WriteTextTest {

	public static void writeTxt(Dataset<Row> ds) throws Exception{
        Dataset<Row> txt = WriteComm.getInstance().writeToTxt(ds,"C:/Users/Administrator/Desktop/sparkFile/role.txt");
        txt.show();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
