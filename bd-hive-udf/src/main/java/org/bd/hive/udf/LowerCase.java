package org.bd.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 字符串转小写类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月4日 下午9:04:19 |创建
 */
public class LowerCase extends UDF {
	 
	/**
	 * <b>描述：</b> 接受单行输入，并产生单行输出
	 * @author wpk | 2017年9月4日 下午9:06:10 |创建
	 * @param s
	 * @return String
	 */
	 public String evaluate(String s) {  
	    if (s == null) {
	    	return null;
	    }
		return s.toLowerCase();  
	 } 
	 
}
