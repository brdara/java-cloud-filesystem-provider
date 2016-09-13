package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;

/**
 * An exception thrown when an iterative operation is cancelled
 */
public class CancelException extends IOException {
	private static final long serialVersionUID = 1L;	

	public CancelException() {
		super();
	}

	public CancelException(String message, Throwable cause) {
		super(message, cause);
	}

	public CancelException(String message) {
		super(message);
	}

	public CancelException(Throwable cause) {
		super(cause);
	}

}

