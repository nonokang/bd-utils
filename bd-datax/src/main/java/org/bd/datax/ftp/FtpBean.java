package org.bd.datax.ftp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import org.bd.datax.DataxBean;
import org.bd.datax.DataxException;
import org.bd.datax.ftp.FtpRead.Parameter;
import org.bd.datax.ftp.FtpRead.Parameter.ColumnIndex;
import org.bd.datax.ftp.FtpRead.Parameter.ColumnValue;
import org.bd.datax.ftp.FtpWrite.WriteParameter;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> ftp脚本转换类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午9:40:31 |创建
 */
@SuppressWarnings("rawtypes")
public class FtpBean implements DataxBean{
	
	public final static String readconf = "ftp读取配置";
	public final static String writeconf = "ftp写入配置";

	/**
	 * <b>描述：</b> 获取读取对象
	 * @author wpk | 2017年10月13日 下午10:03:27 |创建
	 * @param map
	 * @return FtpRead
	 */
	@Override
	public FtpRead readBean(Map map){
		FtpRead bean = new FtpRead();
		Parameter para = bean.new Parameter();

		if(map.containsKey(FtpPara.protocol) && map.get(FtpPara.protocol) != null){
			para.setProtocol(map.get(FtpPara.protocol).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, FtpPara.protocol));
		}
		if(map.containsKey(FtpPara.host) && map.get(FtpPara.host) != null){
			para.setHost(map.get(FtpPara.host).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, FtpPara.host));
		}
		if(map.containsKey(FtpPara.port) && map.get(FtpPara.port) != null){
			para.setPort(Integer.parseInt(map.get(FtpPara.port).toString()));
		}else{
			if("ftp".equals(map.get(FtpPara.protocol).toString())){
				para.setPort(21);
			}else if("sftp".equals(map.get(FtpPara.protocol).toString())){
				para.setPort(22);
			}else{
				throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, FtpPara.port));
			}
		}
		if(map.containsKey(FtpPara.username) && map.get(FtpPara.username) != null){
			para.setUsername(map.get(FtpPara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, FtpPara.username));
		}
		if(map.containsKey(FtpPara.password) && map.get(FtpPara.password) != null){
			para.setPassword(map.get(FtpPara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, FtpPara.password));
		}
		if(map.containsKey(FtpPara.path) && map.get(FtpPara.path) != null){
			List<String> path = new ArrayList<String>();
			String[] str = map.get(FtpPara.path).toString().split(";");
			for(String p : str){
				path.add(p);
			}
			para.setPath(path);
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", readconf, FtpPara.path));
		}
		if(map.containsKey(FtpPara.timeout) && map.get(FtpPara.timeout) != null){
			para.setTimeout(Integer.parseInt(map.get(FtpPara.timeout).toString()));
		}
		if(map.containsKey(FtpPara.connectPattern) && map.get(FtpPara.connectPattern) != null){
			para.setConnectPattern(map.get(FtpPara.connectPattern).toString());
		}
		if(map.containsKey(FtpPara.fieldDelimiter) && map.get(FtpPara.fieldDelimiter) != null){
			para.setFieldDelimiter(map.get(FtpPara.fieldDelimiter).toString());
		}
		if(map.containsKey(FtpPara.compress) && map.get(FtpPara.compress) != null){
			para.setCompress(map.get(FtpPara.compress).toString());
		}
		if(map.containsKey(FtpPara.encoding) && map.get(FtpPara.encoding) != null){
			para.setEncoding(map.get(FtpPara.encoding).toString());
		}
		if(map.containsKey(FtpPara.skipHeader) && map.get(FtpPara.skipHeader) != null){//注意，该值为布尔值
			para.setSkipHeader(Boolean.valueOf(map.get(FtpPara.skipHeader).toString()));
		}
		if(map.containsKey(FtpPara.nullFormat) && map.get(FtpPara.nullFormat) != null){
			para.setNullFormat(map.get(FtpPara.nullFormat).toString());
		}
		if(map.containsKey(FtpPara.maxTraversalLevel) && map.get(FtpPara.maxTraversalLevel) != null){
			para.setMaxTraversalLevel(Integer.parseInt(map.get(FtpPara.maxTraversalLevel).toString()));
		}
		if(map.containsKey(FtpPara.column) && map.get(FtpPara.column) != null){
			if(map.get(FtpPara.column) instanceof JSONArray){
				List<Object> column = new ArrayList<Object>();
				JSONArray ja = (JSONArray)map.get(FtpPara.column);
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
				throw new DataxException(String.format("%s【%s】类型错误,请输入%s类型", readconf, FtpPara.column, new JSONArray().getClass()));
			}
		}else{
			List<Object> column = new ArrayList<Object>();
			column.add("*");
			para.setColumn(column);
		}
		
		bean.setParameter(para);
		return bean;
	}

	/**
	 * <b>描述：</b> 获取写入对象
	 * @author wpk | 2017年10月13日 下午10:15:35 |创建
	 * @param map
	 * @return FtpWrite
	 */
	@Override
	public FtpWrite writeBean(Map map){
		FtpWrite bean = new FtpWrite();
		WriteParameter para = bean.new WriteParameter();
		
		if(map.containsKey(FtpPara.protocol) && map.get(FtpPara.protocol) != null){
			para.setProtocol(map.get(FtpPara.protocol).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.protocol));
		}
		if(map.containsKey(FtpPara.host) && map.get(FtpPara.host) != null){
			para.setHost(map.get(FtpPara.host).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.host));
		}
		if(map.containsKey(FtpPara.port) && map.get(FtpPara.port) != null){
			para.setPort(Integer.parseInt(map.get(FtpPara.port).toString()));
		}else{
			if("ftp".equals(map.get(FtpPara.protocol).toString())){
				para.setPort(21);
			}else if("sftp".equals(map.get(FtpPara.protocol).toString())){
				para.setPort(22);
			}else{
				throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.port));
			}
		}
		if(map.containsKey(FtpPara.username) && map.get(FtpPara.username) != null){
			para.setUsername(map.get(FtpPara.username).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.username));
		}
		if(map.containsKey(FtpPara.password) && map.get(FtpPara.password) != null){
			para.setPassword(map.get(FtpPara.password).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.password));
		}
		if(map.containsKey(FtpPara.path) && map.get(FtpPara.path) != null){
			para.setPath(map.get(FtpPara.path).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.path));
		}
		if(map.containsKey(FtpPara.fileName) && map.get(FtpPara.fileName) != null){
			para.setFileName(map.get(FtpPara.fileName).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.fileName));
		}
		if(map.containsKey(FtpPara.writeMode) && map.get(FtpPara.writeMode) != null){
			para.setWriteMode(map.get(FtpPara.writeMode).toString());
		}else{
			throw new DataxException(String.format("%s【%s】不存在或者为空值", writeconf, FtpPara.writeMode));
		}
		if(map.containsKey(FtpPara.fieldDelimiter) && map.get(FtpPara.fieldDelimiter) != null){
			para.setFieldDelimiter(map.get(FtpPara.fieldDelimiter).toString());
		}
		if(map.containsKey(FtpPara.encoding) && map.get(FtpPara.encoding) != null){
			para.setEncoding(map.get(FtpPara.encoding).toString());
		}
		if(map.containsKey(FtpPara.nullFormat) && map.get(FtpPara.nullFormat) != null){
			para.setNullFormat(map.get(FtpPara.nullFormat).toString());
		}
		if(map.containsKey(FtpPara.timeout) && map.get(FtpPara.timeout) != null){
			para.setTimeout(Integer.parseInt(map.get(FtpPara.timeout).toString()));
		}
		if(map.containsKey(FtpPara.dateFormat) && map.get(FtpPara.dateFormat) != null){
			para.setDateFormat(map.get(FtpPara.dateFormat).toString());
		}
		if(map.containsKey(FtpPara.fileFormat) && map.get(FtpPara.fileFormat) != null){
			para.setFileFormat(map.get(FtpPara.fileFormat).toString());
		}
		if(map.containsKey(FtpPara.header) && map.get(FtpPara.header) != null){
			List<String> header = new ArrayList<String>();
			JSONArray ja = (JSONArray)map.get(FtpPara.header);
			for(int i=0;i<ja.length();i++){
				header.add(ja.getString(i));
			}
			para.setHeader(header);
		}
		
		bean.setParameter(para);
		return bean;
	}
}
