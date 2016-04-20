package com.uk.xarixa.cloud.filesystem.core.host.factory;

import com.uk.xarixa.cloud.filesystem.core.utils.CloudFilesystemRuntimeException;

public class DuplicateCloudHostnameException extends CloudFilesystemRuntimeException {
	private static final long serialVersionUID = -6946337575340307139L;

	public DuplicateCloudHostnameException() {
		super();
	}

	public DuplicateCloudHostnameException(String message) {
		super(message);
	}

	public DuplicateCloudHostnameException(Throwable cause) {
		super(cause);
	}

	public DuplicateCloudHostnameException(String message, Throwable cause) {
		super(message, cause);
	}

	public DuplicateCloudHostnameException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
