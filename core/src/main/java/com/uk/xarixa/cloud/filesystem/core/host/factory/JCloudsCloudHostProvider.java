package com.uk.xarixa.cloud.filesystem.core.host.factory;

import java.net.URI;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfigurationBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;

/**
 * A cloud host provider for JClouds
 */
public class JCloudsCloudHostProvider implements CloudHostProvider {
	public static final String CLOUD_TYPE_ENV = "cloudType";
	private final static Logger LOG = LoggerFactory.getLogger(JCloudsCloudHostProvider.class);
	private final ReentrantReadWriteLock cloudFileSystemsLock = new ReentrantReadWriteLock();
	private Map<String,CloudFileSystem> cloudFileSystems = new HashMap<>();

	@Override
	public CloudFileSystem createCloudFileSystem(FileSystemProvider provider, URI uri, Map<String, ?> env) {
		String cloudType = (String)env.get(CLOUD_TYPE_ENV);
		if (StringUtils.isBlank(cloudType)) {
			throw new IllegalArgumentException("The file system map must contain a '" + CLOUD_TYPE_ENV + "' which identifies the " +
					"cloud type to target. Accepted cloud types are: " + CloudHostConfigurationBuilder.getAllCloudHostSettingTypes());
		}

		// Filter out the cloudType parameter
	    Map<String,?> remainingEnv = 
	    		env.entrySet()
	            .stream()
	            .filter(p -> !p.getKey().equals(CLOUD_TYPE_ENV))
	            .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));

	    LOG.debug("Creating cloud host settings for environment type '{}', filesystem host '{}'", cloudType, uri.getHost());
	    CloudHostConfiguration config = new CloudHostConfigurationBuilder()
			.setType(cloudType)
			.setName(uri.getHost())
			.setAttributes(remainingEnv)
			.build();
	    LOG.debug("Created cloud host settings for environment type '{}', filesystem host '{}'", cloudType, uri.getHost());

	    return createCloudFilesystemInternal(provider, config);
	}

	@Override
	public CloudFileSystem getCloudFileSystem(URI uri) {
		// Check the closed filesystems and remove them from the map
		removeClosedFilesystems();

		cloudFileSystemsLock.readLock().lock();
		
		try {
			return cloudFileSystems.get(uri.getHost());
		} finally {
			cloudFileSystemsLock.readLock().unlock();
		}
	}

	protected CloudFileSystem createCloudFilesystemInternal(FileSystemProvider provider, CloudHostConfiguration config) {
		// Check the closed filesystems and remove them from the map
		removeClosedFilesystems();
    	String fsKey = config.getName();
		cloudFileSystemsLock.readLock().lock();
	    
	    try {
    		// Test if the filesystem has already been created
			if (cloudFileSystems.containsKey(fsKey)) {
	    		throw new DuplicateCloudHostnameException(fsKey);
	    	}

	    	// Upgrade to a write lock
	    	cloudFileSystemsLock.readLock().unlock();
	    	cloudFileSystemsLock.writeLock().lock();
	    	
	    	try {
		    	// Create the blob store
		    	LOG.debug("Creating BlobStoreContext for '{}'", fsKey);
	    	    BlobStoreContext context = config.createBlobStoreContext();
		    	LOG.debug("Created BlobStoreContext for '{}'", fsKey);
	    	    
	    	    // Create the filesystem
		    	LOG.debug("Creating FileSystem for '{}'", fsKey);
	    	    CloudFileSystem fs = new CloudFileSystem(provider, config, context);
		    	LOG.debug("Created FileSystem for '{}'", fsKey);

	    	    // Add it to the map
	    	    cloudFileSystems.put(fsKey, fs);
	    	} finally {
	    	    // Downgrade to a read lock
	    		cloudFileSystemsLock.readLock().lock();
	    		cloudFileSystemsLock.writeLock().unlock();
	    	}
	    	
	    	return cloudFileSystems.get(fsKey);
	    } finally {
	    	cloudFileSystemsLock.readLock().unlock();
	    }
	}
	
	private void removeClosedFilesystems() {
		cloudFileSystemsLock.writeLock().lock();
		
		try {
			// Retain all of the open file systems
			cloudFileSystems = 
		    		cloudFileSystems.entrySet()
		            .stream()
		            .filter(p -> p.getValue().isOpen())
		            .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
		} finally {
			cloudFileSystemsLock.writeLock().unlock();
		}
	}

}
