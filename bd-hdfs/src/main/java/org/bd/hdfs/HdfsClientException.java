package org.bd.hdfs;

public class HdfsClientException extends RuntimeException {

	private static final long serialVersionUID = 1L;

    public HdfsClientException() {
        super();
    }

    public HdfsClientException(String message) {
        super(message);
    }

    public HdfsClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public HdfsClientException(Throwable cause) {
        super(cause);
    }

}
