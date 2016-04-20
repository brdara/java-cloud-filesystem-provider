package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import java.nio.file.FileSystem;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.jclouds.blobstore.BlobStoreContext;

import com.uk.xarixa.cloud.filesystem.core.host.factory.CloudHostProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProviderDelegate;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

/**
 * Defines a bean which implements cloud host settings
 */
public interface CloudHostConfiguration {

	/**
	 * Invoked by {@link CloudFileSystemProviderDelegate} on file system creation to
	 * create the blob store context.
	 * @return
	 * @throws BlobStoreAlreadyCreatedException
	 */
	BlobStoreContext createBlobStoreContext() throws BlobStoreAlreadyCreatedException;

	/**
	 * Retrieves the unique name for this setting. Each setting registered in the {@link CloudHostProvider}
	 * must be unique.
	 * @return
	 */
	String getName();

	/**
	 * Sets the unique name for this setting
	 */
	void setName(String name);

	/**
	 * Gets the cloud host file system specific implementation for these settings
	 * @return
	 */
	CloudFileSystemImplementation getCloudFileSystemImplementation();

	/**
	 * Gets the {@link UserPrincipalLookupService}
	 * @return null if there isn't one, in which case {@link FileSystem#getUserPrincipalLookupService()} will
	 * 			throw an {@link UnsupportedOperationException}
	 */
	UserPrincipalLookupService getUserPrincipalLookupService();

	/**
	 * Gets an instance of the {@link UserGroupLookupService}.
	 * @return
	 */
	UserGroupLookupService<?> getUserGroupLookupService();
	
	/**
	 * Retrieves the security manager for this configuration
	 * @return
	 */
	CloudHostSecurityManager getCloudHostSecurityManager();

	/**
	 * Creates a new {@link WatchService} instance
	 * @return null if there isn't one, in which case {@link FileSystem#newWatchService()} will
	 * 			throw an {@link UnsupportedOperationException}
	 */
	CloudWatchService createWatchService();

	/**
	 * Sets the time in ms that the cloud will be polled for changes when using the {@link #createWatchService() watch service}
	 */
	void setWatchServiceCloudPollTime(long pollTimeMs);

	/**
	 * <p>
	 * Determines whether a delete/copy/move can be performed using native optimised delete/copy/move operations
	 * or whether these operations must be performed using standard non-optimised operations.
	 * </p>
	 * <p>
	 * For example, in AWS, if two different keys can be used to access the same buckets then the implementor
	 * can determine that an optimised operation can be used between this current configuration and the
	 * configuration represented by the other path.
	 * </p>
	 */
	boolean canOptimiseOperationsFor(CloudPath otherCloudPath);

}
