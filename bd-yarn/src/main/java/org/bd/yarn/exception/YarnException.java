package org.bd.yarn.exception;

public class YarnException extends RuntimeException {

	private static final long serialVersionUID = -1520131049273172806L;

    public YarnException() {
        super();
    }

    public YarnException(String message) {
        super(message);
    }

    public YarnException(String message, Throwable cause) {
        super(message, cause);
    }

    public YarnException(Throwable cause) {
        super(cause);
    }

}
