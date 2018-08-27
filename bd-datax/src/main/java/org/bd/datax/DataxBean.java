package org.bd.datax;

import java.util.Map;

import org.bd.datax.bean.Read;
import org.bd.datax.bean.Write;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> datax参数对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午10:22:17 |创建
 */
@SuppressWarnings("rawtypes")
public interface DataxBean {
	
	/**
	 * <b>描述：</b> 获取读取对象
	 * @author wpk | 2017年10月13日 下午10:24:24 |创建
	 * @param map
	 * @return Read
	 */
	public Read readBean(Map map);
	
	/**
	 * <b>描述：</b> 获取写入对象
	 * @author wpk | 2017年10月13日 下午10:25:20 |创建
	 * @param map
	 * @return Write
	 */
	public Write writeBean(Map map);
}
