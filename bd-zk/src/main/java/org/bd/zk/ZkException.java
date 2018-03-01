package org.bd.zk;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> zookeeper运行异常捕获类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月4日 下午9:15:25 |创建
 */
public class ZkException extends RuntimeException {

	private static final long serialVersionUID = 1L;

    public ZkException() {
        super();
    }

    public ZkException(String message) {
        super(message);
    }

    public ZkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkException(Throwable cause) {
        super(cause);
    }

}
