package org.bd.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 字符串转大写类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月4日 下午9:02:22 |创建
 */
public class UpperCase extends UDF {

	/**
	 * <b>描述：</b> 接受单行输入，并产生单行输出
	 * @author wpk | 2017年9月4日 下午9:06:27 |创建
	 * @param s
	 * @return String
	 */
	public String evaluate(String s) {
		if (s == null) {
			return null;
		}
		return s.toUpperCase();
	}
	
}
