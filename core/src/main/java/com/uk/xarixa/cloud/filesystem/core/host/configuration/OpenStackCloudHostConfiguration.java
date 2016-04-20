package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType;

/**
 * Openstack cloud host settings
 */
@CloudHostConfigurationType("OpenStack")
public class OpenStackCloudHostConfiguration extends AbstractDefaultCloudHostConfiguration {
	private final static Logger LOG = LoggerFactory.getLogger(OpenStackCloudHostConfiguration.class);
	private String endpoint;
	private String identity;
	private String credential;

	String getIdentity() {
		return identity;
	}
	public void setIdentity(String identity) {
		this.identity = identity;
	}
	String getCredential() {
		return credential;
	}
	public void setCredential(String credential) {
		this.credential = credential;
	}
	String getEndpoint() {
		return endpoint;
	}
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	@Override
	public BlobStoreContext createBlobStoreContextInternal() {
		LOG.debug("Initialising OpenStack blob view for cloud host settings " + getName());
		return ContextBuilder.newBuilder("openstack-swift")
				.endpoint(getEndpoint())
	            .credentials(getIdentity(), getCredential())
                .buildView(BlobStoreContext.class);
	}
}
