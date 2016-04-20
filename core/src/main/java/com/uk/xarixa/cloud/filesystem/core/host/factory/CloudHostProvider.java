package com.uk.xarixa.cloud.filesystem.core.host.factory;

import java.net.URI;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProviderDelegate;

/**
 * A cloud host provider creates a specialised {@link CloudFileSystem}
 */
public interface CloudHostProvider {

	/**
	 * Creates a new cloud file system
	 * Invoked from {@link CloudFileSystemProviderDelegate#newFileSystem(URI, Map)}
	 * @param uri
	 * @param env
	 * @return
	 */
	CloudFileSystem createCloudFileSystem(FileSystemProvider provider, URI uri, Map<String, ?> env);

	/**
	 * Retrieves a filesystem {@link #createCloudFileSystem(FileSystemProvider, URI, Map) previously created}
	 * with the hostname in the given URI.
	 * @param uri
	 * @return
	 */
	CloudFileSystem getCloudFileSystem(URI uri);

}
