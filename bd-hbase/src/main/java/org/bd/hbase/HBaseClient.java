package org.bd.hbase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.bd.hbase.utils.HBaseUtils;

import com.google.common.collect.Sets;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> hbase客户端<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月8日 下午4:07:21 |创建
 */
public class HBaseClient {

	private static Connection conn;
    
	/**
	 * <b>描述：</b> 获取连接
	 * @author wpk | 2017年11月8日 下午4:29:46 |创建
	 * @return Connection
	 */
    public static Connection getHBaseConnection(){
    	try {
    		if(null == conn || conn.isClosed()){
    	    	Configuration conf = HBaseConfig.getHBaseConfiguration();
    			conn = ConnectionFactory.createConnection(conf);
    		}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return conn;
    }
    
    /**
     * <b>描述：</b> 获取数据库实例
     * @author wpk | 2017年11月8日 下午5:54:54 |创建
     * @return Admin
     */
    public static Admin getAdmin(){
        Admin admin = null;
		try {
			admin = getHBaseConnection().getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return admin;
    }

    /**
     * <b>描述：</b> 判断表是否存在
     * @author wpk | 2017年11月8日 下午4:32:12 |创建
     * @param tableName
     * @throws IOException
     * @return boolean
     */
    public static boolean tableExists(String tableName) {
    	boolean flag = true;
		try {
	        Admin admin = getHBaseConnection().getAdmin();
	        flag = admin.tableExists(TableName.valueOf(tableName));
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flag;
    }
    
    /**
     * <b>描述：</b> 获取表描述对象（HTableDescriptor：包含了表的名字极其对应表的列族）
     * @author wpk | 2017年11月8日 下午4:38:47 |创建
     * @param tableName
     * @return HTableDescriptor
     */
    public static HTableDescriptor getHTableDescriptor(String tableName){
        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor desc = null;
		try {
			Admin admin = getHBaseConnection().getAdmin();
			desc = admin.getTableDescriptor(tn);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return desc;
    }

    /**
     * <b>描述：</b> 获取指定表的所有列族名称
     * @author wpk | 2017年11月8日 下午4:40:33 |创建
     * @param tableName
     * @return Set<String>
     */
    public static Set<String> getFamilyNames(String tableName) {
    	HTableDescriptor desc = getHTableDescriptor(tableName);
        HashSet<String> result = Sets.newHashSet();
        for (byte[] bytes : desc.getFamiliesKeys()) {
            try {
                result.add(new String(bytes, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
            	e.printStackTrace();
            }
        }
        return result;
    }
    
    /**
     * <b>描述：</b> 获取指定列族对象
     * （HColumnDescriptor：维护着关于列族的信息，例如版本号，压缩设置等。
     *   它通常在创建表或者为表添加列族的时候使用。
     *   列族被创建后不能直接修改，只能通过删除然后重新创建的方式。
     *   列族被删除的时候，列族里面的数据也会同时被删除。）
     * @author wpk | 2017年11月8日 下午4:49:20 |创建
     * @param tableName
     * @param familyName
     * @return HColumnDescriptor
     */
    public static HColumnDescriptor getHColumnDescriptor(String tableName, String familyName){
    	Collection<HColumnDescriptor> hcds = getHColumnDescriptors(tableName);
		try {
	    	for(HColumnDescriptor hcd : hcds){
	    		String newFamilyName = new String(hcd.getName(), "UTF-8");
	    		if(newFamilyName.equals(familyName)){
	    			return hcd;
	    		}
	    	}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    	return null;
    }
    
    /**
     * <b>描述：</b> 通过表名获取所有的列族对象
     * @author wpk | 2017年11月8日 下午4:56:56 |创建
     * @param tableName
     * @return Collection<HColumnDescriptor>
     */
    public static Collection<HColumnDescriptor> getHColumnDescriptors(String tableName){
    	HTableDescriptor desc = getHTableDescriptor(tableName);
    	return desc.getFamilies();
    }
    
    /**
     * <b>描述：</b> 创建表或者新增列族<br>
     * （如果该表不存在，则直接创建；<br>
     *   如果存在，则判断是否有新增的列族，有则添加新列族<br>
     * 	   注意1：列族类型支持String,List<String>两种格式<br>
     *   注意2：列族是有大小写区分的）
     * @author wpk | 2017年11月8日 下午5:43:19 |创建
     * @param tableName
     * @param obj
     * @return void
     * @throws UnsupportedDataTypeException 
     */
    public static void createTable(String tableName, Object obj) throws UnsupportedDataTypeException{
    	boolean isExists = tableExists(tableName);
    	List<String> families = HBaseUtils.familyDataTypeConver(obj);
        try {
        	if(isExists){
        		Set<String> existingFamilies = getFamilyNames(tableName);//获取该表的所有列族
        		boolean wait = false;
                if (null != families && families.size() > 0) {
                    for (String family : families) {
                        if (existingFamilies.contains(family) == false) {//判断是否存在新增的列族
                            HColumnDescriptor fd = new HColumnDescriptor(family);
//                          fd.setInMemory(true); // 元数据表存在内存中比较好
                        	getAdmin().addColumn(TableName.valueOf(tableName), fd);
                            wait = true;
                        }
                    }
                }
                //新增后需要等待5秒，确保数据的准确性
                if (wait) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        	}else{
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
                if (null != families && families.size() > 0) {
                    for (String family : families) {
                        HColumnDescriptor fd = new HColumnDescriptor(family);
//                        fd.setInMemory(true); // 元数据表存在内存中比较好
                        desc.addFamily(fd);
                    }
                }
    			getAdmin().createTable(desc);
        	}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * <b>描述：</b> 删除列族<br>
     * （注意1：列族类型支持String,List<String>两种格式<br>
     *   注意2：列族是有大小写区分的）
     * @author wpk | 2017年11月9日 上午11:43:35 |创建
     * @param tableName
     * @param obj
     * @throws UnsupportedDataTypeException
     * @return void
     */
	public static void deleteColumnFamily(String tableName, Object obj) throws UnsupportedDataTypeException{
    	List<String> families = HBaseUtils.familyDataTypeConver(obj);
    	if(families.size() > 0){
    		Set<String> existingFamilies = getFamilyNames(tableName);//获取该表的所有列族
            for (String family : families) {
                if (existingFamilies.contains(family) == true) {//判断是否存在删除的列族
                	try {
						getAdmin().deleteColumn(TableName.valueOf(tableName), family.getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
                }
            }
    	}
    }
    
	/**
	 * <b>描述：</b> 删除表
	 * @author wpk | 2017年11月9日 下午2:36:00 |创建
	 * @param obj
	 * @throws UnsupportedDataTypeException
	 * @return void
	 */
    public static void deleteTable(Object obj) throws UnsupportedDataTypeException{
    	List<String> tables = HBaseUtils.familyDataTypeConver(obj);
    	for(String table : tables){
        	boolean isExists = tableExists(table);
        	if(!isExists) continue;
        	TableName tableName = TableName.valueOf(table);
        	try {
        		getAdmin().disableTable(tableName);
    			getAdmin().deleteTable(tableName);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    }
    
    /**
     * <b>描述：</b> 获取所有表名称
     * @author wpk | 2017年11月9日 下午2:52:15 |创建
     * @return List<String>
     */
    public static List<String> listTableNames(){
    	List<String> list = new ArrayList<String>();
    	try {
			TableName[] tns = getAdmin().listTableNames();
			for(int i=0; i<tns.length; i++){
				list.add(tns[i].getNameAsString());
				System.out.println(tns[i].getNameAsString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return list;
    }
}
