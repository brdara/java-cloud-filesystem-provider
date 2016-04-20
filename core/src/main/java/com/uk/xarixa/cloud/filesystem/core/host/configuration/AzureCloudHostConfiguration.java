package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType;

/**
 * Azure cloud host settings
 */
@CloudHostConfigurationType("Azure")
public class AzureCloudHostConfiguration extends AbstractDefaultCloudHostConfiguration {
	private final static Logger LOG = LoggerFactory.getLogger(AzureCloudHostConfiguration.class);
	private String storageAccountName;
	private String storageAccountKey;

	public String getStorageAccountName() {
		return storageAccountName;
	}
	public void setStorageAccountName(String storageAccountName) {
		this.storageAccountName = storageAccountName;
	}
	public String getStorageAccountKey() {
		return storageAccountKey;
	}
	public void setStorageAccountKey(String storageAccountKey) {
		this.storageAccountKey = storageAccountKey;
	}
	@Override
	public BlobStoreContext createBlobStoreContextInternal() {
		LOG.debug("Initialising Azure blob view for cloud host settings {}", getName());
		return ContextBuilder.newBuilder("azureblob")
				.credentials(getStorageAccountName(), getStorageAccountKey())
                .buildView(BlobStoreContext.class);
	}

}
