package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType;
import com.uk.xarixa.cloud.filesystem.core.nio.AwsCloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation;

/**
 * AWS cloud settings
 */
@CloudHostConfigurationType("AWS")
public class AwsCloudHostConfiguration extends AbstractDefaultCloudHostConfiguration {
	private final static Logger LOG = LoggerFactory.getLogger(AwsCloudHostConfiguration.class);
	private static AwsCloudFileSystemImplementation awsCloudFileSystemImplementation = new AwsCloudFileSystemImplementation();
	private String accessKey;
	private String secretKey;

	String getAccessKey() {
		return accessKey;
	}
	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}
	String getSecretKey() {
		return secretKey;
	}
	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
	@Override
	public CloudFileSystemImplementation getDefaultCloudFileSystemImplementation() {
		return awsCloudFileSystemImplementation;
	}
	@Override
	public BlobStoreContext createBlobStoreContextInternal() {
		LOG.debug("Initialising AWS blob view for cloud host settings {}", getName());
		return ContextBuilder.newBuilder("aws-s3")
                .credentials(getAccessKey(), getSecretKey())
                .buildView(BlobStoreContext.class);

	}
}
