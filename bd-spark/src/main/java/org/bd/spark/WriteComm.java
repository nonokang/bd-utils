package org.bd.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.bd.spark.enums.DbmsType;
import org.bd.spark.enums.FormatType;
import org.bd.spark.utils.PropertiesUtil;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 写入公共类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年7月23日上午2:02:18 |创建
 */
public class WriteComm {
	
	public volatile static WriteComm swc;
	
	/**
	 * <b>描述：获取实例</b>
	 * @author wpk | 2017年7月23日上午2:06:12 |创建
	 * @return
	 */
	public static WriteComm getInstance() {
		if (swc == null){
			synchronized (WriteComm.class) {
				if(swc == null){
					swc = new WriteComm();
				}
			}
		}
		return swc;
	}
	
	/**
	 * <b>描述：把结果集写入数据库</b>
	 * @author wpk | 2017年7月23日上午2:10:09 |创建
	 * @param ds
	 * @param dbType	数据库类型（该值应与配置文件中的参数前缀一致）
	 * @param tableName	表名（把数据写入指定表中）
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> writeToJDBC(Dataset<Row> ds, DbmsType dbType, String tableName) throws Exception{
		return writeToJDBC(ds, dbType, tableName, SaveMode.Overwrite);
	}
	
	/**
	 * <b>描述：把结果集写入数据库</b>
	 * @author wpk | 2017年7月25日下午3:57:18 |创建
	 * @param ds
	 * @param dbType	数据库类型（该值应与配置文件中的参数前缀一致）
	 * @param tableName	表名（把数据写入指定表中）
	 * @param saveMode	写入模式
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> writeToJDBC(Dataset<Row> ds, DbmsType dbType, String tableName, SaveMode saveMode) throws Exception{
		try {
			Map<String,String> map = PropertiesUtil.getInstance().getValueByFile("jdbc.properties", dbType.value());
			if(map!=null){
				map.put("dbtable",tableName);
		        ds.write().mode(saveMode).format(FormatType.JDBC.getValue()).options(map).save();
			}
		} catch (Exception e) {
			throw new Exception(e);
		}
		return ds;
	}
	
	/**
	 * <b>描述：写入文件</b>
	 * @author wpk | 2017年7月25日下午4:24:25 |创建
	 * @param ds
	 * @param format	文件类型
	 * @param path		文件路径
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> writeToFile(Dataset<Row> ds, FormatType format, String path) throws Exception{
		return writeToFile(ds, SaveMode.Overwrite, format, path);
	}
	
	/**
	 * <b>描述：写入文件</b>
	 * @author wpk | 2017年7月25日下午4:14:01 |创建
	 * @param ds
	 * @param saveMode	写入模式
	 * @param format	文件类型
	 * @param path		文件路径
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> writeToFile(Dataset<Row> ds, SaveMode saveMode, FormatType format, String path) throws Exception{
		try {
			ds.write().mode(saveMode).format(format.getValue()).save(path);
		} catch (Exception e) {
			throw new Exception(e);
		}
		return ds;
	}
	
	/**
	 * <b>描述：写入txt文件</b>
	 * @author wpk | 2017年7月25日下午10:41:10 |创建
	 * @param ds
	 * @param path	文件路径
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> writeToTxt(Dataset<Row> ds, String path) throws Exception{
		return writeToTxt(ds, SaveMode.Overwrite, path, "|");
	}
	
	/**
	 * <b>描述：写入txt文件</b>
	 * @author wpk | 2017年7月25日下午10:39:16 |创建
	 * @param ds
	 * @param saveMode	写入模式
	 * @param path		文件路径
	 * @param separator	字段分割符（缺省时，默认使用“|”作为分隔符）
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> writeToTxt(Dataset<Row> ds, SaveMode saveMode, String path, String separator) throws Exception{
		try {
			Dataset<String> newDS = txtTransformat(ds, separator);
			newDS.write().mode(saveMode).text(path);
		} catch (Exception e) {
			throw new Exception(e);
		}
		return ds;
	}
	
	/**
	 * <b>描述：通过转换获取新的结果集</b>
	 * @author wpk | 2017年7月25日下午10:41:48 |创建
	 * @param ds
	 * @param separator	字段分隔符
	 * @return
	 * @throws Exception
	 */
	public Dataset<String> txtTransformat(Dataset<Row> ds, String separator) throws Exception{
		Dataset<String> mapDS = null;
		try {
	        List<String> list = Arrays.asList(ds.columns());
	        Encoder<String> stringEncoder = Encoders.STRING();
	        mapDS = ds.map((MapFunction<Row, String>) row -> list.stream().map(x -> row.getAs(x).toString()).collect(Collectors.joining(separator)),stringEncoder);
	        ds.toJavaRDD();
		} catch (Exception e) {
			throw new Exception(e);
		}
		return mapDS;
	}
	
}
