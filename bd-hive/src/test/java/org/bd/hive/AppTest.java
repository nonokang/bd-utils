package org.bd.hive;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;


/**
 * Unit test for simple App.
 */
public class AppTest {
	
    public static void main( String[] arg ) throws Exception {
    	IHiveClient ihc = new HiveClient();
    	ResultSet rs = ihc.executeQuery("show partitions wpk_test.test11");
		ResultSetMetaData metaData = rs.getMetaData();
		int count = metaData.getColumnCount();
		List<String> list = new ArrayList<String>();
		for(int i=0;i<count;i++){
			list.add(metaData.getColumnName(i+1));
			System.out.print(metaData.getColumnName(i+1)+"\t");
		}
    }
}
