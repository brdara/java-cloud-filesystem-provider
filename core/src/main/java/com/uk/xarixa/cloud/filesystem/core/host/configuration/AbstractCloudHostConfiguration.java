package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.jclouds.blobstore.BlobStoreContext;

/**
 * An abstract class for {@link CloudHostConfiguration} which wraps some default methods
 */
public abstract class AbstractCloudHostConfiguration implements CloudHostConfiguration, Comparable<AbstractCloudHostConfiguration> {
	private final AtomicBoolean blobStoreCreated = new AtomicBoolean(false);
	private String name;

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	@Override
	public final boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!obj.getClass().isAssignableFrom(AbstractCloudHostConfiguration.class)) {
			return false;
		}

		AbstractCloudHostConfiguration other = (AbstractCloudHostConfiguration)obj;
		return new EqualsBuilder()
				.append(getName(), other.getName())
				.isEquals();
	}
	
	@Override
	public final int hashCode() {
		return getName().hashCode();
	}

	@Override
	public final int compareTo(AbstractCloudHostConfiguration other) {
		return getName().compareTo(other.getName());
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the settings name from {@link CloudHostConfiguration#getName()} annotation
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * Ensures that the {@link BlobStoreContext} is only created once
	 */
	@Override
	public final BlobStoreContext createBlobStoreContext() throws BlobStoreAlreadyCreatedException {
		if (!blobStoreCreated.compareAndSet(false, true)) {
			throw new BlobStoreAlreadyCreatedException();
		}

		return createBlobStoreContextInternal();
	}

	/**
	 * Invoked by {@link #createBlobStoreContext()} to create the {@link BlobStoreContext}
	 * @return
	 */
	protected abstract BlobStoreContext createBlobStoreContextInternal();

}
