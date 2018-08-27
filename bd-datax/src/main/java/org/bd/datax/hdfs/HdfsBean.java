package org.bd.datax.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import org.bd.datax.DataxBean;
import org.bd.datax.DataxException;
import org.bd.datax.hdfs.HdfsRead.Parameter;
import org.bd.datax.hdfs.HdfsRead.Parameter.ColumnIndex;
import org.bd.datax.hdfs.HdfsRead.Parameter.ColumnValue;
import org.bd.datax.hdfs.HdfsRead.Parameter.CsvReaderConfig;
import org.bd.datax.hdfs.HdfsWrite.WriteParameter;
import org.bd.datax.hdfs.HdfsWrite.WriteParameter.WriteColumn;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> hdfs脚本转换类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午5:20:24 |创建
 */
@SuppressWarnings("rawtypes")
public class HdfsBean implements DataxBean{
	
	public final static String readconf = "hdfs读取配置";
	public final static String writeconf = "hdfs写入配置";

	/**
	 * <b>描述：</b> 获取读取对象
	 * @author wpk | 2017年10月13日 下午5:47:43 |创建
	 * @param map
	 * @return HdfsRead
	 */
	@Override
	public HdfsRead readBean(Map map){
		HdfsRead bean = new HdfsRead();
		Parameter para = bean.new Parameter();

		if(map.containsKey(HdfsPara.defaultFS) && map.get(HdfsPara.defaultFS) != null){
			para.setDefaultFS(map.get(HdfsPara.defaultFS).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, HdfsPara.defaultFS));
		}
		if(map.containsKey(HdfsPara.path) && map.get(HdfsPara.path) != null){
			para.setPath(map.get(HdfsPara.path).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, HdfsPara.path));
		}
		if(map.containsKey(HdfsPara.column) && map.get(HdfsPara.column) != null){
			if(map.get(HdfsPara.column) instanceof JSONArray){
				List<Object> column = new ArrayList<Object>();
				JSONArray ja = (JSONArray)map.get(HdfsPara.column);
				for(int i=0;i<ja.length();i++){
					JSONObject jo = ja.getJSONObject(i);
					if(jo.has("index")){
						ColumnIndex col = para.new ColumnIndex();
						col.setIndex(jo.getInt("index"));
						col.setType(jo.getString("type"));
						column.add(col);
					}else if(jo.has("value")){
						ColumnValue col = para.new ColumnValue();
						col.setValue(jo.getString("value"));
						col.setType(jo.getString("type"));
						column.add(col);
					}
				}
				para.setColumn(column);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", readconf, HdfsPara.column, JSONArray.class));
			}
		}else{
			List<Object> column = new ArrayList<Object>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(HdfsPara.fileType) && map.get(HdfsPara.fileType) != null){
			para.setFileType(map.get(HdfsPara.fileType).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, HdfsPara.fileType));
		}
		if(map.containsKey(HdfsPara.fieldDelimiter) && map.get(HdfsPara.fieldDelimiter) != null && !map.get(HdfsPara.fieldDelimiter).equals("")){
			para.setFieldDelimiter(map.get(HdfsPara.fieldDelimiter).toString());
		}
		if(map.containsKey(HdfsPara.compress) && map.get(HdfsPara.compress) != null){
			para.setCompress(map.get(HdfsPara.compress).toString());
		}
		if(map.containsKey(HdfsPara.encoding) && map.get(HdfsPara.encoding) != null){
			para.setEncoding(map.get(HdfsPara.encoding).toString());
		}
		if(map.containsKey(HdfsPara.nullFormat) && map.get(HdfsPara.nullFormat) != null){
			para.setNullFormat(map.get(HdfsPara.nullFormat).toString());
		}
		if(map.containsKey(HdfsPara.haveKerberos) && (Boolean)map.get(HdfsPara.haveKerberos) == true){
			para.setHaveKerberos(Boolean.valueOf(map.get(HdfsPara.haveKerberos).toString()));
			if(map.containsKey(HdfsPara.kerberosKeytabFilePath) && map.get(HdfsPara.kerberosKeytabFilePath) != null){
				para.setKerberosKeytabFilePath(map.get(HdfsPara.kerberosKeytabFilePath).toString());
			}else{
				throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, HdfsPara.kerberosKeytabFilePath));
			}
			if(map.containsKey(HdfsPara.kerberosPrincipal) && map.get(HdfsPara.kerberosPrincipal) != null){
				para.setKerberosPrincipal(map.get(HdfsPara.kerberosPrincipal).toString());
			}else{
				throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, HdfsPara.kerberosPrincipal));
			}
		}
		if(map.containsKey(HdfsPara.csvReaderConfig) && map.get(HdfsPara.csvReaderConfig) != null){
			if(map.get(HdfsPara.csvReaderConfig) instanceof JSONObject){
				CsvReaderConfig crc = para.new CsvReaderConfig();
				JSONObject jo = (JSONObject)map.get(HdfsPara.csvReaderConfig);
				if(jo.has("caseSensitive")){
					crc.setCaseSensitive(Boolean.valueOf(jo.getBoolean("caseSensitive")));
				}
				if(jo.has("textQualifier")){
					crc.setTextQualifier((char)jo.getInt("textQualifier"));
				}
				if(jo.has("trimWhitespace")){
					crc.setTrimWhitespace(Boolean.valueOf(jo.getBoolean("trimWhitespace")));
				}
				if(jo.has("useTextQualifier")){
					crc.setUseTextQualifier(Boolean.valueOf(jo.getBoolean("useTextQualifier")));
				}
				if(jo.has("delimiter")){
					crc.setDelimiter((char)jo.getInt("delimiter"));
				}
				if(jo.has("recordDelimiter")){
					crc.setRecordDelimiter((char)jo.getInt("recordDelimiter"));
				}
				if(jo.has("comment")){
					crc.setComment((char)jo.getInt("comment"));
				}
				if(jo.has("useComments")){
					crc.setUseComments(Boolean.valueOf(jo.getBoolean("useComments")));
				}
				if(jo.has("escapeMode")){
					crc.setEscapeMode(jo.getInt("escapeMode"));
				}
				if(jo.has("safetySwitch")){
					crc.setSafetySwitch(Boolean.valueOf(jo.getBoolean("safetySwitch")));
				}
				if(jo.has("skipEmptyRecords")){
					crc.setSkipEmptyRecords(Boolean.valueOf(jo.getBoolean("skipEmptyRecords")));
				}
				if(jo.has("captureRawRecord")){
					crc.setCaptureRawRecord(Boolean.valueOf(jo.getBoolean("captureRawRecord")));
				}
				para.setCsvReaderConfig(crc);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", readconf, HdfsPara.csvReaderConfig, JSONObject.class));
			}
		}
		
		bean.setParameter(para);
		return bean;
	}
	
	/**
	 * <b>描述：</b> 获取写入对象
	 * @author wpk | 2017年10月13日 下午9:39:32 |创建
	 * @param map
	 * @return HdfsWrite
	 */
	@Override
	public HdfsWrite writeBean(Map map) {
		HdfsWrite bean = new HdfsWrite();
		WriteParameter para = bean.new WriteParameter();

		if(map.containsKey(HdfsPara.defaultFS) && map.get(HdfsPara.defaultFS) != null){
			para.setDefaultFS(map.get(HdfsPara.defaultFS).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.defaultFS));
		}
		if(map.containsKey(HdfsPara.path) && map.get(HdfsPara.path) != null){
			para.setPath(map.get(HdfsPara.path).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.path));
		}
		if(map.containsKey(HdfsPara.column) && map.get(HdfsPara.column) != null){
			if(map.get(HdfsPara.column) instanceof JSONArray){
				List<Object> column = new ArrayList<Object>();
				JSONArray ja = (JSONArray)map.get(HdfsPara.column);
				for(int i=0;i<ja.length();i++){
					WriteColumn col = para.new WriteColumn();
					JSONObject jo = ja.getJSONObject(i);
					col.setName(jo.getString("name"));
					col.setType(jo.getString("type"));
					column.add(col);
				}
				para.setColumn(column);
			}else{
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", writeconf, HdfsPara.column, JSONArray.class));
			}
		}else{
			List<Object> column = new ArrayList<Object>();
			column.add("*");
			para.setColumn(column);
		}
		if(map.containsKey(HdfsPara.fileType) && map.get(HdfsPara.fileType) != null){
			para.setFileType(map.get(HdfsPara.fileType).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.fileType));
		}
		if(map.containsKey(HdfsPara.fieldDelimiter) && map.get(HdfsPara.fieldDelimiter) != null && !map.get(HdfsPara.fieldDelimiter).equals("")){
			para.setFieldDelimiter(map.get(HdfsPara.fieldDelimiter).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.fieldDelimiter));
		}
		if(map.containsKey(HdfsPara.compress) && map.get(HdfsPara.compress) != null){
			para.setCompress(map.get(HdfsPara.compress).toString());
		}
		if(map.containsKey(HdfsPara.encoding) && map.get(HdfsPara.encoding) != null){
			para.setEncoding(map.get(HdfsPara.encoding).toString());
		}
		if(map.containsKey(HdfsPara.haveKerberos) && (Boolean)map.get(HdfsPara.haveKerberos) == true){
			para.setHaveKerberos(Boolean.valueOf(map.get(HdfsPara.haveKerberos).toString()));
			if(map.containsKey(HdfsPara.kerberosKeytabFilePath) && map.get(HdfsPara.kerberosKeytabFilePath) != null){
				para.setKerberosKeytabFilePath(map.get(HdfsPara.kerberosKeytabFilePath).toString());
			}else{
				throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.kerberosKeytabFilePath));
			}
			if(map.containsKey(HdfsPara.kerberosPrincipal) && map.get(HdfsPara.kerberosPrincipal) != null){
				para.setKerberosPrincipal(map.get(HdfsPara.kerberosPrincipal).toString());
			}else{
				throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.kerberosPrincipal));
			}
		}
		if(map.containsKey(HdfsPara.fileName) && map.get(HdfsPara.fileName) != null){
			para.setFileName(map.get(HdfsPara.fileName).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.fileName));
		}
		if(map.containsKey(HdfsPara.writeMode) && map.get(HdfsPara.writeMode) != null){
			para.setWriteMode(map.get(HdfsPara.writeMode).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, HdfsPara.writeMode));
		}
		
		bean.setParameter(para);
		return bean;
	}
	
}
