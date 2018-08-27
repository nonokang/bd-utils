package org.bd.datax;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> datax异常类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月13日 下午4:44:46 |创建
 */
public class DataxException extends RuntimeException {

	private static final long serialVersionUID = -1520131049273172806L;

    public DataxException() {
        super();
    }

    public DataxException(String message) {
        super(message);
    }

    public DataxException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataxException(Throwable cause) {
        super(cause);
    }
}
