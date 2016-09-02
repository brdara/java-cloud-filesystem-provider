package com.uk.xarixa.cloud.filesystem.core.nio;

/**
 * An exception thrown when  the {@link CloudPath} is invalid
 */
public class CloudPathException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public CloudPathException() {
		super();
		
	}

	public CloudPathException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		
	}

	public CloudPathException(String message, Throwable cause) {
		super(message, cause);
		
	}

	public CloudPathException(String message) {
		super(message);
		
	}

	public CloudPathException(Throwable cause) {
		super(cause);
		
	}

}
