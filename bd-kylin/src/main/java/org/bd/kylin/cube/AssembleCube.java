package org.bd.kylin.cube;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bd.kylin.cube.Aggregation_groups.Select_rule;
import org.bd.kylin.cube.Hbase_mapping.Column_family;
import org.bd.kylin.cube.Hbase_mapping.Columns;
import org.bd.kylin.cube.Measures.Function;
import org.bd.kylin.cube.Measures.Parameter;
import org.bd.kylin.cube.Rowkey.Rowkey_columns;
import org.bd.kylin.utils.JsonBinder;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 组装cube<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月14日 下午3:05:47 |创建
 */
public class AssembleCube {
	
	private static AssembleCube assembleCube = null;
	
	public static AssembleCube getInstance(){
		if(assembleCube == null){
			assembleCube = new AssembleCube();
		}
		return assembleCube;
	}

	/**
	 * <b>描述：</b> cube对象转化为cube脚本
	 * @author wpk | 2017年9月14日 下午3:21:45 |创建
	 * @param cubeJson
	 * @throws Exception
	 * @return String
	 */
	public static String cubeObjTojson(CubeJson cubeJson) throws Exception{
		ObjectMapper om = JsonBinder.buildNormalBinder().getMapper();
		String str = "";
		try {
			str = om.writeValueAsString(cubeJson);
		} catch (Exception e) {
			throw new Exception(String.format("转化异常：%s",e));
		}
		return str;
	}
	
	public static List<Dimensions> getDimensions(String source){
		JSONArray ary = new JSONArray(source);
		List<Dimensions> list = new ArrayList<Dimensions>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = (JSONObject)ary.get(i);
			Dimensions item = new Dimensions();
			item.setColumn(obj.getString("column").toUpperCase());//需转大写
			item.setName(obj.getString("name"));
			item.setTable(obj.getString("table").toUpperCase());//需转大写
			item.setDerived(null);
			list.add(item);
		}
		return list;
	}
	
	public static List<Measures> getMeasures(String source){
		JSONArray ary = new JSONArray(source);
		List<Measures> list = new ArrayList<Measures>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = (JSONObject)ary.get(i);
			String name = obj.getString("name");
			String returntype = obj.getString("returntype");
			String type = obj.getString("type");
			String value = obj.getString("value");
			JSONArray expression = obj.getJSONArray("expression");
			for(int j = 0; j<expression.length() ; j++){
				Measures item = new Measures();
				Parameter pa = item.new Parameter();
				pa.setType(type);
				pa.setValue(value.toUpperCase());//需转大写
				pa.setNext_parameter(null);
				
				Function fu = item.new Function();
				fu.setExpression((String)expression.get(j));
				fu.setReturntype(returntype);
				fu.setParameter(pa);

				item.setName(name+"_"+(String)expression.get(j));
				item.setFunction(fu);
				list.add(item);
			}
		}
		return list;
	}
	
	public static Rowkey getRowkey(String source){
		JSONArray ary = new JSONArray(source);
		Rowkey item = new Rowkey();

		List<Rowkey_columns> rowkey_columns = new ArrayList<Rowkey_columns>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = ary.getJSONObject(i);
			Rowkey_columns rc = item.new Rowkey_columns();
			rc.setColumn(obj.getString("column").toUpperCase());//需转大写
			rowkey_columns.add(rc);
		}
		
		item.setRowkey_columns(rowkey_columns);
		return item;
	}
	
	public static List<Aggregation_groups> getAggregation_groups(String source){
		List<Aggregation_groups> list = new ArrayList<Aggregation_groups>();
		JSONArray ary = new JSONArray(source);
		for(int i = 0; i<ary.length(); i++){
			Aggregation_groups item = new Aggregation_groups();
			JSONObject obj = (JSONObject)ary.get(i);
			JSONArray includesAry = obj.getJSONArray("includes");//维度基数
			JSONArray mandatoryAry = obj.getJSONArray("mandatory_dims");//必要维度
			JSONArray hierarchyAry = obj.getJSONArray("hierarchy_dims");//层级维度
			JSONArray jointAry = obj.getJSONArray("joint_dims");//联合维度
			List<String> includesList = new ArrayList<String>();
			List<String> mandatoryList = new ArrayList<String>();
			List<List<String>> hierarchyList = new ArrayList<List<String>>();
			List<List<String>> jointList = new ArrayList<List<String>>();
			for(int j = 0; j<includesAry.length(); j++){
				JSONObject o = includesAry.getJSONObject(j);
				String val = (String)o.get("field");
				includesList.add(val.toUpperCase());//需转大写
			}
			for(int j = 0; j<mandatoryAry.length(); j++){
				JSONObject o = mandatoryAry.getJSONObject(j);
				String val = (String)o.get("field");
				mandatoryList.add(val.toUpperCase());//需转大写
			}
			for(int j = 0; j<hierarchyAry.length(); j++){
				JSONArray a = hierarchyAry.getJSONArray(j);
				List<String> aList = new ArrayList<String>();
				for(int k = 0; k<a.length(); k++){
					JSONObject o = a.getJSONObject(k);
					String val = (String)o.get("field");
					aList.add(val.toUpperCase());//需转大写
				}
				hierarchyList.add(aList);
			}
			for(int j = 0; j<jointAry.length(); j++){
				JSONArray a = jointAry.getJSONArray(j);
				List<String> aList = new ArrayList<String>();
				for(int k = 0; k<a.length(); k++){
					JSONObject o = a.getJSONObject(k);
					String val = (String)o.get("field");
					aList.add(val.toUpperCase());//需转大写
				}
				jointList.add(aList);
			}

			Select_rule select_rule = item.new Select_rule();
			select_rule.setMandatory_dims(mandatoryList);
			select_rule.setHierarchy_dims(hierarchyList);
			select_rule.setJoint_dims(jointList);
			item.setIncludes(includesList);
			item.setSelect_rule(select_rule);
			list.add(item);
		}
		return list;
	}
	
	public static List<Notify_list> getNotify_list(){
		List<Notify_list> list = new ArrayList<Notify_list>();
//		Notify_list item = new Notify_list();
//		list.add(item);
		return list;
	}
	
	public static Hbase_mapping getHbase_mapping(String source){
		JSONArray ary = new JSONArray(source);
		Hbase_mapping item = new Hbase_mapping();
		List<Column_family> column_family = new ArrayList<Column_family>();
		List<String> measure_refs = new ArrayList<String>();
		for(int i = 0; i<ary.length(); i++){
			JSONObject obj = (JSONObject)ary.get(i);
			String name = obj.getString("name");
			JSONArray expression = obj.getJSONArray("expression");
			for(int j = 0; j<expression.length() ; j++){
				measure_refs.add(name+"_"+(String)expression.get(j));
			}
		}
		if(measure_refs.size() > 0){
			Column_family cf = item.new Column_family();
			
			List<Columns> cList = new ArrayList<Columns>();
			Columns c = item.new Columns();
			c.setMeasure_refs(measure_refs);
			cList.add(c);
			
			cf.setColumns(cList);
			
			column_family.add(cf);
			item.setColumn_family(column_family);
		}
		return item;
	}
	
}