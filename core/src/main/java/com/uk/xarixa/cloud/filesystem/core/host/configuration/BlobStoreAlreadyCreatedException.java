package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import com.uk.xarixa.cloud.filesystem.core.utils.CloudFilesystemRuntimeException;

public class BlobStoreAlreadyCreatedException extends CloudFilesystemRuntimeException {
	private static final long serialVersionUID = -1094171455697995894L;

	public BlobStoreAlreadyCreatedException() {
		super();
	}

	public BlobStoreAlreadyCreatedException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public BlobStoreAlreadyCreatedException(String message, Throwable cause) {
		super(message, cause);
	}

	public BlobStoreAlreadyCreatedException(String message) {
		super(message);
	}

	public BlobStoreAlreadyCreatedException(Throwable cause) {
		super(cause);
	}

}
