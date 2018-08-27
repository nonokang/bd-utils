package org.bd.spark;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bd.spark.enums.DbmsType;
import org.bd.spark.enums.FormatType;
import org.bd.spark.utils.PropertiesUtil;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 读取源数据公共类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月23日上午2:01:02 |创建
 */
public class ReadComm {

	//常量定义
	public final static String JDBC = "jdbc";
	
	public volatile static ReadComm src;
	
	/**
	 * <b>描述：获取实例</b>
	 * @author wpk | 2017年7月23日上午2:06:12 |创建
	 * @return
	 */
	public static ReadComm getInstance() {
		if (src == null){
			synchronized (ReadComm.class) {
				if(src == null){
					src = new ReadComm();
				}
			}
		}
		return src;
	}
	
	/**
	 * <b>描述：通过jdbc获取数据集</b>
	 * @author wpk | 2017年7月23日上午1:08:51 |创建
	 * @param spark
	 * @param dbType	数据库类型（该值应与配置文件中的参数前缀一致）
	 * @param tableName	表名（从指定表中读取数据）
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> readByJDBC(SparkSession spark,DbmsType dbType, String tableName) throws Exception{
		Dataset<Row> ds = null;
		try {
			Map<String,String> map = PropertiesUtil.getInstance().getValueByFile("jdbc.properties", dbType.value());
			if(map!=null){
				map.put("dbtable",tableName);
		        ds = spark.read().format(FormatType.JDBC.getValue()).options(map).load();
			}
		} catch (Exception e) {
			throw new Exception(e);
		}
		return ds;
	}
	
	/**
	 * <b>描述：
	 * 加载单个文件
	 * 在有hadoop的环境下，是从hdfs的路径上加载文件
	 * </b>
	 * @author wpk | 2017年7月25日上午11:31:23 |创建
	 * @param spark
	 * @param format	加载文件类型
	 * @param path		加载路径
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> readSource(SparkSession spark,FormatType format,String path) throws Exception{
		Dataset<Row> ds = null;
		try {
			 ds = spark.read().format(format.getValue()).load(path);
		} catch (Exception e) {
			throw new Exception(e);
		}
		return ds;
	}
	
	/**
	 * <b>描述：
	 * 加载多个文件(把多个文件的数据整合到一起)
	 * 在有hadoop的环境下，是从hdfs的路径上加载文件
	 * </b>
	 * @author wpk | 2017年7月25日下午2:30:11 |创建
	 * @param spark
	 * @param format	加载文件类型
	 * @param paths		加载多个路径
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> readSource(SparkSession spark,String format,String... paths) throws Exception{
		Dataset<Row> ds = null;
		try {
			 ds = spark.read().format(format).load(paths);
		} catch (Exception e) {
			throw new Exception(e);
		}
		return ds;
	}
	
}
