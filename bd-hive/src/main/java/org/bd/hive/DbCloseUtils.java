package org.bd.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 数据库连接关闭类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月31日 上午10:05:10 |创建
 */
public class DbCloseUtils {

    private static final Logger logger = LoggerFactory.getLogger(DbCloseUtils.class);
    
    /**
     * <b>描述：</b> 关闭结果集
     * @author wpk | 2017年10月31日 上午10:05:53 |创建
     * @param rs
     * @return void
     */
    public static void close(final ResultSet rs) {
    	close((AutoCloseable) rs);
    }

    /**
     * <b>描述：</b> 关闭声明
     * @author wpk | 2017年10月31日 上午10:06:27 |创建
     * @param stmt
     * @return void
     */
    public static void close(final Statement stmt) {
    	close((AutoCloseable) stmt);
    }

    /**
     * <b>描述：</b> 关闭连接
     * @author wpk | 2017年10月31日 上午10:06:41 |创建
     * @param conn
     * @return void
     */
    public static void close(final Connection conn) {
    	close((AutoCloseable) conn);
    }

    /**
     * <b>描述：</b> 
     * @author wpk | 2017年10月31日 上午10:07:03 |创建
     * @param closeable
     * @return void
     */
    public static void close(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
            logger.error("", ioe);
        }
    }
}
