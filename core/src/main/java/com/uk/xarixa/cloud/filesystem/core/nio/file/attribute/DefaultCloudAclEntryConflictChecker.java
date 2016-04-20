package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.security.acl.Group;
import java.security.acl.NotOwnerException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.uk.xarixa.cloud.filesystem.core.security.AnonymousGroupPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.utils.ResettingAclIterator;

/**
 * <p>
 * This class is built to recognise and understand the meaning of classes which are/are subclasses of the following:
 * <ul>
 * <li>{@link UserPrincipal}
 * <li>{@link GroupPrincipal}
 * <li>{@link PublicPrivateCloudPermissionsPrincipal}
 * </ul>
 * In addition it is aware of the following types:
 * <ul>
 * <li>{@link AnonymousUserPrincipal}
 * <li>{@link AnonymousGroupPrincipal}
 * </ul>
 * </p>
 * <p>
 * A default implementation for ACL conflicts. This checks if:
 * <ul>
 * <li>Both entries have a {@link CloudAclEntry#getPrincipalClass() principal class}
 * of type {@link PublicPrivateCloudPermissionsPrincipal}, one entry is a {@link AclEntryType#ALLOW} and the other entry
 * is a {@link AclEntryType#DENY}, and that for both entries{@link PublicPrivateCloudPermissionsPrincipal#getBlobAccess()}
 * is equal.
 * <li>Both entries have a {@link CloudAclEntry#getPrincipalClass() principal class}
 * of type {@link UserPrincipal}, and that both principals are {@link UserPrincipal#equals(Object) equals}, and permissions
 * conflict.
 * <li>Both entries have a {@link CloudAclEntry#getPrincipalClass() principal class}
 * of type {@link GroupPrincipal}, and that both principals are {@link GroupPrincipal#equals(Object) equals}, and permissions
 * conflict.
 * </ul>
 * </p>
 * <p>
 * An optional check can be implemented by invoking the non-default constructor
 * {@link #DefaultCloudAclEntryConflictChecker(boolean) DefaultCloudAclEntryConflictChecker(true)} which adds the following
 * checks:
 * <ul>
 * <li>The {@link CloudAclEntry#getPrincipalClass() principal classes} are of the type {@link UserPrincipal} and {@link Group},
 * and the {@link Group#isMember(Principal)} returns true for the user on this group, and permissions conflict.
 * </ul>
 * Set this to <em>true</em> if user ACL's cannot override group ACL's in your scheme. The default (<em>false</em>) is fine to
 * use in most normal cases where a user ACL can override a group ACL.
 * </p>
 * <p>
 * Permissions conflict if one entry is a {@link AclEntryType#ALLOW} and the other entry
 * is a {@link AclEntryType#DENY} and {@link CloudAclEntry#getPermissions()} contains an equivalent permission in each of the
 * two entries being checked. Permissions conflict checks are performed in the
 * {@link #checkAllowDenyRulesHaveConflictingPermissions(CloudAclEntry, CloudAclEntry)} method.
 * </p>
 */
public class DefaultCloudAclEntryConflictChecker implements CloudAclEntryConflictChecker {
	@SuppressWarnings("unchecked")
	private final static Class<? extends Principal>[] PRINCIPAL_CLASSES =
			(Class<? extends Principal>[])new Class<?>[] {PublicPrivateCloudPermissionsPrincipal.class, UserPrincipal.class, GroupPrincipal.class};
	private final boolean checkGroupMembership;
	public static final CloudAclEntryConflictChecker INSTANCE = new DefaultCloudAclEntryConflictChecker();

	public DefaultCloudAclEntryConflictChecker() {
		this(false);
	}

	public DefaultCloudAclEntryConflictChecker(boolean checkGroupMembership) {
		this.checkGroupMembership = checkGroupMembership;
	}
	

	/**
	 * Requires that the set passed in does not use a fail-fast iterator
	 */
	@Override
	public void mergeAcls(Principal aclOwner, CloudAclEntrySet acls) {
		// Return the current set if there is only one ACL
		if (acls.size() > 1) {
			forEachMergeableCloudAclEntry(acls,
					c -> {
						try {
							// Remove old ACL's, add new ones
							acls.removeEntry(aclOwner, c.getEntry1());
							acls.removeEntry(aclOwner, c.getEntry2());
							acls.addEntry(aclOwner, mergeAcl(c));
						} catch (NotOwnerException e) {
							throw new RuntimeException(e);
						}
					});
		}
	}

	/**
	 * Requires that the set passed in does not use a fail-fast iterator
	 */
	@Override
	public CloudAclEntrySet mergeAcls(Principal aclOwner, CloudAclEntry<?>... acls) {
		CloudAclEntrySet entrySet = new CloudAclEntrySet(aclOwner, this, acls);
		mergeAcls(aclOwner, entrySet);
		return entrySet;
	}

	/**
	 * Merges two ACL's together
	 * @param conflictingCloudAclEntry
	 * @return
	 */
	protected CloudAclEntry<?> mergeAcl(ConflictingCloudAclEntry conflictingCloudAclEntry) {
		CloudAclEntry<?> entry1 = conflictingCloudAclEntry.getEntry1();
		CloudAclEntry<?> entry2 = conflictingCloudAclEntry.getEntry2();
		
		// Merge into entry1
		entry1.getPermissions().addAll(entry2.getPermissions());
		entry1.getFlags().addAll(entry2.getFlags());

		return entry1;
	}
	
	/**
	 * Performs the consumer action for each conflicting entry
	 * @param acls
	 * @param consumerAction
	 */
	protected void forEachMergeableCloudAclEntry(CloudAclEntrySet acls, Consumer<ConflictingCloudAclEntry> consumerAction) {
		iterateAcrossAllAcls(false, acls, (entry1 ,entry2) ->
									{
										if (isMergeableAcl(entry1, entry2)) {
											consumerAction.accept(new ConflictingCloudAclEntry(entry1, entry2));
										}
									});
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean isMergeableAcl(CloudAclEntry<?> original, CloudAclEntry<?> other) {
		if (isConflictingAcl(original, other)) {
			return false;
		}

		// Are the classes the same?
		if (original.getPrincipalClass().equals(other.getPrincipalClass())) {

			// It only makes sense to have one of these as permissions
			if (original.getPrincipalClass().isAssignableFrom(PublicPrivateCloudPermissionsPrincipal.class)) {
				PublicPrivateCloudPermissionsPrincipal perm1 = (PublicPrivateCloudPermissionsPrincipal)original.getPrincipal();
				PublicPrivateCloudPermissionsPrincipal perm2 = (PublicPrivateCloudPermissionsPrincipal)other.getPrincipal();
				return perm1.getBlobAccess().equals(perm2.getBlobAccess());
			}

			// Check if the two group ACL's are equivalent
			if (GroupPrincipal.class.isAssignableFrom(original.getPrincipalClass())) {
				return checkPrincipalsCanMerge((CloudAclEntry<GroupPrincipal>)original, (CloudAclEntry<GroupPrincipal>)other);
			}

			// Check if two user ACL's conflict with deny/allow
			if (UserPrincipal.class.isAssignableFrom(original.getPrincipalClass())) {
				return checkPrincipalsCanMerge((CloudAclEntry<UserPrincipal>)original, (CloudAclEntry<UserPrincipal>)other);
			}

		} else if (checkGroupMembership) {
			// Check if the user is member of the group, then check the ACL's therein
			CloudAclEntry<UserPrincipal> userPrincipal = getCloudAclEntryWithPrincipalClass(UserPrincipal.class, original, other);
			
			if (userPrincipal != null) {
				CloudAclEntry<GroupPrincipal> groupPrincipal = getCloudAclEntryWithPrincipalClass(GroupPrincipal.class, original, other);
				
				// Check if there is a group principal, it has group membership methods (from Group)
				// and that the user is a member of the group
				if (groupPrincipal != null && Group.class.isAssignableFrom(groupPrincipal.getPrincipal().getClass()) &&
						((Group)groupPrincipal.getPrincipal()).isMember(userPrincipal.getPrincipal())) {
					return checkPrincipalsCanMerge(userPrincipal, groupPrincipal);
				}
			}
		}

		return false;
	}

	/**
	 * Performs an iteration across all ACL's in the set. This uses the {@link ResettingAclIterator} to iterate
	 * across the ACL set for the particular {@link #PRINCIPAL_CLASSES}. For each match it's guaranteed that the
	 * two entries will not be iterated across more than once.
	 * @param iterateByType		When true this will iterate and compare by the types in {@link #PRINCIPAL_CLASSES},
	 * 							when false this will iterate and compare every single entry against each other (only once)
	 * @param acls
	 * @param consumerAction	An action which accepts two CloudAclEntry types
	 */
	protected void iterateAcrossAllAcls(boolean iterateByType, CloudAclEntrySet acls,
			BiConsumer<CloudAclEntry<?>, CloudAclEntry<?>> consumerAction) {
		// Iterate across the ACL's
		ResettingAclIterator iterator1 = new ResettingAclIterator(acls);
		ResettingAclIterator iterator2 = new ResettingAclIterator(acls);
		Set<Integer> iteratedAcls = new HashSet<>();

		if (iterateByType) {
			// Go through each ACL class
			for (Class<? extends Principal> principalClass : PRINCIPAL_CLASSES) {
				iterateAcrossAllAcls(iterator1, iterator2, consumerAction, iteratedAcls, principalClass);
			}
		} else {
			iterateAcrossAllAcls(iterator1, iterator2, consumerAction, iteratedAcls, null);
		}
	}

	private void iterateAcrossAllAcls(ResettingAclIterator iterator1, ResettingAclIterator iterator2,
			BiConsumer<CloudAclEntry<?>, CloudAclEntry<?>> consumerAction, Set<Integer> iteratedAcls,
			Class<? extends Principal> principalClass) {
		iterator1.reset(principalClass);

		// First iterator produces an entry
		while (iterator1.hasNext()) {
			iterator2.reset(principalClass);
			CloudAclEntry<?> entry1 = iterator1.next();

			// Second iterator produces an entry
			while (iterator2.hasNext()) {
				CloudAclEntry<?> entry2 = iterator2.next();
				
				// If the objects are different and we haven't already iterated across them then perform the action
				if (entry1 != entry2 && iteratedAcls.add(entry1.hashCode() + entry2.hashCode())) {
					consumerAction.accept(entry1, entry2);
				}
			}

		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean isConflictingAcl(CloudAclEntry<?> original, CloudAclEntry<?> other) {
		// Are the classes the same?
		if (original.getPrincipalClass().equals(other.getPrincipalClass())) {

			// It only makes sense to have one of these as permissions
			if (original.getPrincipalClass().isAssignableFrom(PublicPrivateCloudPermissionsPrincipal.class)) {
				PublicPrivateCloudPermissionsPrincipal perm1 = (PublicPrivateCloudPermissionsPrincipal)original.getPrincipal();
				PublicPrivateCloudPermissionsPrincipal perm2 = (PublicPrivateCloudPermissionsPrincipal)other.getPrincipal();
				return !perm1.getBlobAccess().equals(perm2.getBlobAccess());
			}

			// Check if the two group ACL's conflict
			if (GroupPrincipal.class.isAssignableFrom(original.getPrincipalClass())) {
				return checkPrincipalsConflict((CloudAclEntry<GroupPrincipal>)original, (CloudAclEntry<GroupPrincipal>)other);
			}

			// Check if two user ACL's conflict with deny/allow
			if (UserPrincipal.class.isAssignableFrom(original.getPrincipalClass())) {
				return checkPrincipalsConflict((CloudAclEntry<UserPrincipal>)original, (CloudAclEntry<UserPrincipal>)other);
			}

		} else if (checkGroupMembership) {
			// Check if the user is member of the group, then check the ACL's therein
			CloudAclEntry<UserPrincipal> userPrincipal = getCloudAclEntryWithPrincipalClass(UserPrincipal.class, original, other);
			
			if (userPrincipal != null) {
				CloudAclEntry<GroupPrincipal> groupPrincipal = getCloudAclEntryWithPrincipalClass(GroupPrincipal.class, original, other);
				
				// Check if there is a group principal, it has group membership methods (from Group)
				// and that the user is a member of the group
				if (groupPrincipal != null && Group.class.isAssignableFrom(groupPrincipal.getPrincipal().getClass()) &&
						((Group)groupPrincipal.getPrincipal()).isMember(userPrincipal.getPrincipal())) {
					return checkAllowDenyRulesHaveConflictingPermissions(userPrincipal, groupPrincipal);
				}
			}
		}

		return false;
	}
	
	@SuppressWarnings("unchecked")
	protected <T extends Principal> CloudAclEntry<T> getCloudAclEntryWithPrincipalClass(Class<T> principalClass,
			CloudAclEntry<?>... entries) {
		for (CloudAclEntry<?> entry : entries) {
			if (principalClass.isAssignableFrom(entry.getPrincipalClass())) {
				return (CloudAclEntry<T>)entry;
			}
		}
		
		return null;
	}
	
	/**
	 * Standard checks on the principal to check if one principal equals the other and both have the same
	 * {@link CloudAclEntry#getType() allow/deny type}
	 * @param original
	 * @param other
	 * @return true if the principals can be merged, otherwise false
	 * @see #checkAllowDenyRulesHaveConflictingPermissions(CloudAclEntry, CloudAclEntry)
	 */
	protected boolean checkPrincipalsCanMerge(CloudAclEntry<? extends Principal> original, CloudAclEntry<? extends Principal> other) {
		// Are these the same users?
		if (original.getPrincipal().equals(other.getPrincipal())) {
			return !isAllowDenyType(original.getType(), other.getType());
		}

		return false;
	}

	/**
	 * Standard checks on the principal to check if one principal equals the other and have different
	 * {@link CloudAclEntry#getType() allow/deny type}
	 * @param original
	 * @param other
	 * @return true if the principals conflict, otherwise false
	 * @see #checkAllowDenyRulesHaveConflictingPermissions(CloudAclEntry, CloudAclEntry)
	 */
	protected boolean checkPrincipalsConflict(CloudAclEntry<? extends Principal> original, CloudAclEntry<? extends Principal> other) {
		// Are these the same users?
		if (original.getPrincipal().equals(other.getPrincipal())) {
			return checkAllowDenyRulesHaveConflictingPermissions(original, other);
		}

		return false;
	}

	/**
	 * Checks if one entry is for a {@link AclEntryType#ALLOW} and another is for a {@link AclEntryType#DENY} and
	 * the {@link CloudAclEntry#getPermissions()} contains any one permissions which is the same in each entry.
	 * @param original
	 * @param other
	 * @return
	 * @see #isAllowDenyType(AclEntryType, AclEntryType)
	 */
	protected boolean checkAllowDenyRulesHaveConflictingPermissions(CloudAclEntry<? extends Principal> original,
			CloudAclEntry<? extends Principal> other) {
		// Is one rule an allow and another a deny?
		if (isAllowDenyType(original.getType(), other.getType())) {
			// Check perms to see if there are conflicts
			if (original.getPermissions().stream().anyMatch(p -> other.getPermissions().contains(p))) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Checks if one {@link AclEntryType type} is an {@link AclEntryType#ALLOW} rule and another {@link AclEntryType type}
	 * is a {@link AclEntryType#DENY} rule
	 * @param type1
	 * @param type2
	 * @return
	 */
	protected boolean isAllowDenyType(AclEntryType type1, AclEntryType type2) {
		return ( (AclEntryType.ALLOW.equals(type1) && AclEntryType.DENY.equals(type2)) ||
				 (AclEntryType.DENY.equals(type1) && AclEntryType.ALLOW.equals(type2)) );
			
	}

	/**
	 * @return true if the group membership check is on, false if not
	 */
	public boolean isCheckGroupMembership() {
		return checkGroupMembership;
	}

}
