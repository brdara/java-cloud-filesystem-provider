package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.security.Principal;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.jclouds.blobstore.domain.BlobAccess;

/**
 * A permissions principal which denotes if there is {@link BlobAccess#PRIVATE} or
 * {@link BlobAccess#PUBLIC_READ} access.
 */
public class PublicPrivateCloudPermissionsPrincipal implements Principal, Comparable<PublicPrivateCloudPermissionsPrincipal> {
	private final BlobAccess blobAccess;

	public PublicPrivateCloudPermissionsPrincipal(BlobAccess blobAccess) {
		this.blobAccess = blobAccess;
	}

	@Override
	public String getName() {
		return blobAccess.name();
	}

	/**
	 * Returns the generic cloud permission
	 */
	public BlobAccess getBlobAccess() {
		return blobAccess;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	@Override
	public int compareTo(PublicPrivateCloudPermissionsPrincipal other) {
		return blobAccess.compareTo(other.blobAccess);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (!(obj instanceof PublicPrivateCloudPermissionsPrincipal)) {
			return false;
		}

		return compareTo((PublicPrivateCloudPermissionsPrincipal)obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(blobAccess)
				.toHashCode();
	}

}
