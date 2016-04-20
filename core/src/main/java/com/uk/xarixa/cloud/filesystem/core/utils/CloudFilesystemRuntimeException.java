package com.uk.xarixa.cloud.filesystem.core.utils;

/**
 * A root for runtime exceptions thrown in this project
 */
public class CloudFilesystemRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 3215786830519475367L;

	public CloudFilesystemRuntimeException() {
		super();
	}

	public CloudFilesystemRuntimeException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CloudFilesystemRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public CloudFilesystemRuntimeException(String message) {
		super(message);
	}

	public CloudFilesystemRuntimeException(Throwable cause) {
		super(cause);
	}

}
