package org.bd.hbase;

import java.io.IOException;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> hbase客户端<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月8日 上午8:54:06 |创建
 */
public class Test {

    public static void main(String[] arg) throws IOException{
//    	createTable("wpk_test1","col1");
    	
    	/*List<String> list = new ArrayList<String>();
    	list.add("col1");
    	list.add("col2");
    	list.add("COL3");
    	list.add("CoL4");
        HBaseClient.createTable("wpk_test", list);
        System.out.println("新增结束...");
        HBaseClient.createTable("wpk_test", "codls5");*/
        
//        HBaseClient.deleteColumnFamily("wpk_test", "codls5");
    	
//    	HBaseClient.deleteTable("member");
    	
    	HBaseClient.listTableNames();
        
        /*Collection<HColumnDescriptor> c = HBaseClient.getHColumnDescriptors("wpk_test");
        for(HColumnDescriptor d : c){
        	System.out.println(d.getNameAsString());
        }*/
    }
}
