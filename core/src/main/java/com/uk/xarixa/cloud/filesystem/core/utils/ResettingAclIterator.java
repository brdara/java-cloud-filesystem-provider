package com.uk.xarixa.cloud.filesystem.core.utils;

import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;

/**
 * A performant iterator which filters through a list based on a {@link Principal} class
 * which is matched against {@link CloudAclEntry#getPrincipalClass()}. The iterator can be
 * {@link #reset(Class)} against the same {@link CloudAclEntrySet} with multiple classes.
 * This is more memory performant in terms of searching than creating subsets with a particular class.
 */
public class ResettingAclIterator implements Iterator<CloudAclEntry<?>> {		
	private final CloudAclEntrySet aclEntries;
	private Iterator<CloudAclEntry<?>> currentIterator;
	private Class<? extends Principal> principalClass;
	private CloudAclEntry<?> nextEntry;
	private boolean hasNext;

	public ResettingAclIterator(CloudAclEntrySet aclEntries) {
		this.aclEntries = aclEntries;
	}
	
	public ResettingAclIterator reset(Class<? extends Principal> principalClass) {
		this.principalClass = principalClass;
		currentIterator = aclEntries.iterator();
		return this;
	}

	@Override
	public boolean hasNext() {
		return hasNextInternal(true);
	}
	
	boolean hasNextInternal(boolean advance) {
		while (advance && (hasNext = currentIterator.hasNext())) {
			nextEntry = currentIterator.next();

			// When no principal class then iterate across the whole set
			if (principalClass == null) {
				return hasNext;
			}

			// GroupPrincipal inherits from UserPrincipal so we would like to constrain the
			// Class.isAssignableFrom call when we are looking for UserPrincipal to exclude GroupPrincipal
			boolean lookingForUserPrincipal = UserPrincipal.class.isAssignableFrom(principalClass) &&
					!GroupPrincipal.class.isAssignableFrom(principalClass);

			// Check if this is the class type we are looking for
			if (principalClass.isAssignableFrom(nextEntry.getPrincipalClass()) &&
					(!lookingForUserPrincipal || !GroupPrincipal.class.isAssignableFrom(nextEntry.getPrincipalClass()))) {
				return hasNext;
			}
		}

		return hasNext;
	}

	@Override
	public CloudAclEntry<?> next() {
		if (!hasNextInternal(false)) {
			throw new NoSuchElementException();
		}

		return nextEntry;
	}
	
}