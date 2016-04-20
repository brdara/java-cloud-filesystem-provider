package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import java.nio.file.attribute.UserPrincipalLookupService;

import org.jclouds.blobstore.BlobStoreContext;

import com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.AbstractCloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

@CloudHostConfigurationType("mock-test")
public class MockCloudHostConfiguration extends AbstractCloudHostConfiguration {

	@Override
	public CloudFileSystemImplementation getCloudFileSystemImplementation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean canOptimiseOperationsFor(CloudPath otherCloudPath) {
		return this.equals(otherCloudPath.getFileSystem().getCloudHostConfiguration());
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloudWatchService createWatchService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setWatchServiceCloudPollTime(long pollTimeMs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CloudHostSecurityManager getCloudHostSecurityManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserGroupLookupService<?> getUserGroupLookupService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected BlobStoreContext createBlobStoreContextInternal() {
		// TODO Auto-generated method stub
		return null;
	}

}
