package com.uk.xarixa.cloud.filesystem.core.nio.options;

import java.nio.file.CopyOption;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation.CloudMethod;

/**
 * Extended cloud copy options
 */
public enum CloudCopyOption implements CopyOption {
	/**
	 * Perform recursive copy/move if directories have been specified
	 */
	RECURSIVE,
	
	/**
	 * Does not return the {@link CloudMethod} used for the copy
	 */
	DONT_RETURN_COPY_METHOD,
	
	/**
	 * Don't throw an exception on a copy error but fail silently and continue
	 */
	FAIL_SILENTLY;

}
