package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.security.Principal;
import java.util.Set;

/**
 * A conflict check which can be set on the {@link CloudAclFileAttributes} to determine conflicts
 */
public interface CloudAclEntryConflictChecker {

	/**
	 * Determines if the two ACL's conflict, for example if one of the entries allows access at a particular
	 * level and the other doesn't. This is used in the {@link CloudAclEntrySet} to determine if members
	 * can be added.
	 * @param original
	 * @param other
	 * @return
	 */
	boolean isConflictingAcl(CloudAclEntry<?> original, CloudAclEntry<?> other);

	/**
	 * Determines if the two ACL's can be merged. That is, the two entries do not conflict and they
	 * contain an entry for the same {@link CloudAclEntry#getPrincipal() principal} and {@link CloudAclEntry#getType() entry type}
	 * but with different {@link CloudAclEntry#getPermissions() permissions} or {@link CloudAclEntry#getFlags() flags}.
	 * @param original
	 * @param other
	 * @return
	 */
	boolean isMergeableAcl(CloudAclEntry<?> original, CloudAclEntry<?> other);

	/**
	 * Attempts to merge a set of ACL's into as smaller set of ACL's. For example, two
	 * ACL's which allow a user different permissions can be merged into a single ACL
	 * for the user.
	 * @param acls
	 * @return
	 * @throws IllegalStateException If there are conflicting ACL's in the set
	 */
	void mergeAcls(Principal aclOwner, CloudAclEntrySet acls);

	/**
	 * @see #mergeAcls(Principal, Set)
	 */
	CloudAclEntrySet mergeAcls(Principal aclOwner, CloudAclEntry<?>... acls);

}
