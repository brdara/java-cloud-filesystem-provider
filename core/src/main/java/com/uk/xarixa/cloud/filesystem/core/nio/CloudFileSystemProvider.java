package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;
import java.util.Set;

import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

/**
 * An enhanced mixin interface for {@link FileSystemProvider} which enhances that functionality
 */
public interface CloudFileSystemProvider {

	/**
	 * Delete a single path with the given delete options
	 * @param path
	 * @param options
	 * @throws IOException
	 */
	void delete(CloudPath path, DeleteOption... options) throws IOException;

	/**
	 * Delete multiple paths with the given delete options
	 * @param path
	 * @param options
	 * @throws IOException
	 */
	void delete(Set<CloudPath> path, DeleteOption... options) throws IOException;

	/**
	 * Delete multiple paths with the given delete options
	 * @param path
	 * @param options
	 * @throws IOException
	 */
	void delete(Set<CloudPath> path, EnumSet<DeleteOption> options) throws IOException;

	/**
	 * Copy multiple paths to the target
	 * @param source
	 * @param target
	 * @param options
	 * @throws IOException
	 */
	void copy(Set<CloudPath> source, Path target, CopyOption... options) throws IOException;

	/**
	 * Copy multiple paths to the target
	 * @param source
	 * @param target
	 * @param options
	 * @throws IOException
	 */
	void copy(Set<CloudPath> source, Path target, Set<CopyOption> options) throws IOException;

}
