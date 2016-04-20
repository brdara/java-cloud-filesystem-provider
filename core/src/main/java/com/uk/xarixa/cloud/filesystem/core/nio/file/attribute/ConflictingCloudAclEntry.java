package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Provides a conflicting pair of ACL entries
 */
public class ConflictingCloudAclEntry {
	private CloudAclEntry<?> entry1;
	private CloudAclEntry<?> entry2;

	ConflictingCloudAclEntry(CloudAclEntry<?> entry1, CloudAclEntry<?> entry2) {
		this.entry1 = entry1;
		this.entry2 = entry2;
	}

	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (!(obj instanceof ConflictingCloudAclEntry)) {
			return false;
		}
		
		return entry1.equals(entry2);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
			.append(entry1.hashCode())
			.append(entry2.hashCode())
			.toHashCode();
	}
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	public CloudAclEntry<?> getEntry1() {
		return entry1;
	}

	public void setEntry1(CloudAclEntry<?> entry1) {
		this.entry1 = entry1;
	}

	public CloudAclEntry<?> getEntry2() {
		return entry2;
	}

	public void setEntry2(CloudAclEntry<?> entry2) {
		this.entry2 = entry2;
	}

}
