package com.uk.xarixa.cloud.filesystem.core.nio.options;

import java.nio.file.Files;

public enum DeleteOption {

	/**
	 * Performs a recursive delete, which is more optimal than using
	 * {@link Files#walkFileTree(java.nio.file.Path, java.nio.file.FileVisitor)} as it only uses a single
	 * directory listing remote method invocation.
	 */
	RECURSIVE,

	/**
	 * Don't throw an exception on a delete error but fail silently and continue
	 */
	FAIL_SILENTLY

}
