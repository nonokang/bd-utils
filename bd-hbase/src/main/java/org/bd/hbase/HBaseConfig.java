package org.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.bd.hbase.utils.Consts;
import org.bd.hbase.utils.SysVarsUtils;


/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 连接hbase数据库配置<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月8日 下午4:22:54 |创建
 */
public class HBaseConfig{
	
    private static final ThreadLocal<Configuration> configThreadLocal = new ThreadLocal<Configuration>();
	
    /**
     * <b>描述：</b> 创建hbase连接配置
     * @author wpk | 2017年11月8日 下午4:23:23 |创建
     * @return Configuration
     */
    private static Configuration createHBaseConfiguration() {
		SysVarsUtils sysVarsUtils = SysVarsUtils.getInstance();

        Configuration conf = HBaseConfiguration.create();
        
        conf.set("hbase.zookeeper.property.clientPort", sysVarsUtils.getVarByName(Consts.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)); 
        conf.set("hbase.zookeeper.quorum", sysVarsUtils.getVarByName(Consts.HBASE_ZOOKEEPER_QUORUM)); 
        conf.set("hbase.master", sysVarsUtils.getVarByName(Consts.HBASE_MASTER)); 
	
        return conf;
    }

    /**
     * <b>描述：</b> 获取hbase连接配置
     * @author wpk | 2017年11月8日 下午4:23:44 |创建
     * @return Configuration
     */
    public synchronized static Configuration getHBaseConfiguration() {
        if (configThreadLocal.get() == null) {
        	Configuration config = createHBaseConfiguration();
            configThreadLocal.set(config);
        }
        return configThreadLocal.get();
    }
	
}