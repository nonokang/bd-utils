package org.bd.kylin;

public class CubeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

    public CubeException() {
        super();
    }

    public CubeException(String message) {
        super(message);
    }

    public CubeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CubeException(Throwable cause) {
        super(cause);
    }

}
