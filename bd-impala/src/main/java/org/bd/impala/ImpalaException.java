package org.bd.impala;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> hive运行异常捕获类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月31日 下午2:48:35 |创建
 */
public class ImpalaException extends RuntimeException {

	private static final long serialVersionUID = 1L;

    public ImpalaException() {
        super();
    }

    public ImpalaException(String message) {
        super(message);
    }

    public ImpalaException(String message, Throwable cause) {
        super(message, cause);
    }

    public ImpalaException(Throwable cause) {
        super(cause);
    }

}
