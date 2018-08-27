package org.bd.kylin.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bd.kylin.model.Lookups.Join;
import org.bd.kylin.utils.JsonBinder;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 组装model<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:47:33 |创建
 */
public class AssembleModel {
	
	private static AssembleModel assembleModel = null;
	
	public static AssembleModel getInstance(){
		if(assembleModel == null){
			assembleModel = new AssembleModel();
		}
		return assembleModel;
	}
	
	/**
	 * <b>描述：</b> cube对象转化为cube脚本
	 * @author wpk | 2017年9月14日 下午3:21:45 |创建
	 * @param cubeJson
	 * @throws Exception
	 * @return String
	 */
	public static String modelObjTojson(ModelJson modelJson) throws Exception{
		ObjectMapper om = JsonBinder.buildNormalBinder().getMapper();
		String str = "";
		try {
			str = om.writeValueAsString(modelJson);
		} catch (Exception e) {
			throw new Exception(String.format("转化异常：%s",e));
		}
		return str;
	}
	
	public static List<Lookups> getLookups(String source){
		JSONArray ary = new JSONArray(source);
		List<Lookups> list = new ArrayList<Lookups>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = (JSONObject)ary.get(i);
			Lookups item = new Lookups();
			
			Join join = item.new Join();
			join.setType(obj.getString("type"));
			JSONArray primary_key = obj.getJSONArray("primary_key");//事实表的字段
			JSONArray foreign_key = obj.getJSONArray("foreign_key");//查询表的字段
			List<String> primaryKey = new ArrayList<String>();
			List<String> foreignKey = new ArrayList<String>();
			for(int j=0;j<primary_key.length();j++){
				String column = (String)primary_key.get(j);
				primaryKey.add(column.toUpperCase());//需转大写
			}
			for(int j=0;j<foreign_key.length();j++){
				String column = (String)foreign_key.get(j);
				foreignKey.add(column.toUpperCase());//需转大写
			}
			join.setPrimary_key(foreignKey);
			join.setForeign_key(primaryKey);
			
			item.setTable(obj.getString("table").toUpperCase());//需转大写
			item.setJoin(join);
			
			list.add(item);
		}
		return list;
	}
	
	public static List<Dimensions> getDimensions(String source){
		JSONArray ary = new JSONArray(source);
		List<Dimensions> list = new ArrayList<Dimensions>();
		Map<String,List<String>> map = new HashMap<String,List<String>>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = (JSONObject)ary.get(i);
			String table = obj.getString("table").toUpperCase();//需转大写
			if(!map.containsKey(table)){
				List<String> l = new ArrayList<String>();
				l.add(obj.getString("column").toUpperCase());//需转大写
				map.put(table, l);
			}else{
				List<String> l = map.get(table);
				l.add(obj.getString("column").toUpperCase());//需转大写
				map.put(table, l);
			}
		}
		for(String key : map.keySet()){
			List<String> l = map.get(key);
			Dimensions item = new Dimensions();
			
			item.setTable(key);
			item.setColumns(l);
			
			list.add(item);
		}
		return list;
	}
	
	public static List<String> getMetrics(String source){
		JSONArray ary = new JSONArray(source);
		List<String> list = new ArrayList<String>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = (JSONObject)ary.get(i);
			String type = obj.getString("type");
			if("column".equals(type)){
				String value = obj.getString("value").toUpperCase();//需转大写
				list.add(value);
			}
		}
		return list;
	}
	
	public static Partition_desc getPartition_desc(String source){
		JSONObject obj = new JSONObject(source);
		Partition_desc item = new Partition_desc();
		if(obj.length() > 0){
			if(obj.has("partition_date_column")){
				item.setPartition_date_column(obj.getString("partition_date_column"));
			}
			if(obj.has("partition_date_format")){
				item.setPartition_date_format(obj.getString("partition_date_format"));
			}
		}
		return item;
	}
	
}
