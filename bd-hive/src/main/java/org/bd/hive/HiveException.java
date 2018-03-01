package org.bd.hive;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> hive运行异常捕获类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月31日 下午2:48:35 |创建
 */
public class HiveException extends RuntimeException {

	private static final long serialVersionUID = 1L;

    public HiveException() {
        super();
    }

    public HiveException(String message) {
        super(message);
    }

    public HiveException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveException(Throwable cause) {
        super(cause);
    }

}
