package org.bd.hbase.utils;

import java.util.ArrayList;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> hbase工具类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月9日 下午2:23:45 |创建
 */
public class HBaseUtils {

	/**
	 * <b>描述：</b> 参数转换
	 * @author wpk | 2017年11月9日 下午2:24:31 |创建
	 * @param obj
	 * @throws UnsupportedDataTypeException
	 * @return List<String>
	 */
    @SuppressWarnings("unchecked")
	public static List<String> familyDataTypeConver(Object obj) throws UnsupportedDataTypeException{
    	List<String> list = new ArrayList<String>();
    	if(obj instanceof String){
    		list.add((String)obj);
    	}else if(obj instanceof List){
			List<Object> objList = (List<Object>)obj;
			if(null == objList || objList.size() == 0) return list;
			Object _obj= objList.get(0);
			boolean flag = _obj instanceof String;
			if(!flag){
	    		String str = String.format("参数不支持%s类型转%s类型", _obj.getClass().getName(), String.class.getName());
	    		throw new UnsupportedDataTypeException(str);
			}else{
				list.addAll((List<String>)obj);
			}
    	}else{
    		String str = String.format("参数不支持%s类型", obj.getClass().getName());
    		throw new UnsupportedDataTypeException(str);
    	}
    	return list;
    }
}
