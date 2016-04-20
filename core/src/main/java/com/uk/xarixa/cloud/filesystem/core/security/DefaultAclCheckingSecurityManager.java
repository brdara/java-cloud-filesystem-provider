package com.uk.xarixa.cloud.filesystem.core.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.utils.IsAllowed;

/**
 * <p>
 * A security manager which simply checks the ACL list against the permissions. No underlying
 * security manager is used, the permissions are worked out in-situ from the ACL's.
 * </p>
 * <p>
 * Calculates if the user/group has access to this resource by checking for:
 * <ul>
 * <li>If the <em>currentUser</em> is not empty and is a {@link UserPrincipal}:
 * <ul>
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for this user
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for a group that the user belongs to with
 * no other ACL with a {@link AclEntryType#DENY} for the <em>currentUser</em>
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for an {@link AnonymousUserPrincipal}
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for an {@link AnonymousGroupPrincipal} with no
 * other ACL with the {@link AclEntry#DENY} for the <em>currentUser</em> or {@link AnonymousUserPrincipal}
 * </ul>
 * <li>If the <em>currentUser</em> is not empty and is a {@link GroupPrincipal}:
 * <ul>
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for this group
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for an {@link AnonymousUserPrincipal}
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for an {@link AnonymousGroupPrincipal} with no
 * other ACL with the {@link AclEntry#DENY} for the <em>currentUser</em> or {@link AnonymousUserPrincipal}
 * </ul>
 * <li>If the <em>currentUser</em> is null/empty:
 * <ul>
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for an {@link AnonymousUserPrincipal}
 * <li>An ACL is found with {@link AclEntryType#ALLOW} for an {@link AnonymousGroupPrincipal} with no
 * other ACL with the {@link AclEntry#DENY} for the {@link AnonymousUserPrincipal}
 * </ul>
 * </ul>
 * In the absence of any permissions then the access is denied unless the user is the file owner.
 * </p>
 * 
 * @see AnonymousUserPrincipal
 * @see AnonymousGroupPrincipal
 */
public class DefaultAclCheckingSecurityManager implements CloudHostSecurityManager {
	private final static Logger LOG = LoggerFactory.getLogger(CloudFileAttributesView.class);
	private final UserGroupLookupService<?> userPrincipalLookupService;

	public DefaultAclCheckingSecurityManager(UserGroupLookupService<?> userPrincipalLookupService) {
		this.userPrincipalLookupService = userPrincipalLookupService;
	}

	@Override
	public boolean checkAccessAllowed(CloudPath path, UserPrincipal userOrGroup,
			Set<AclEntryPermission> checkPermissions) {
		// Get the view for the path
		CloudFileAttributesView cloudFileAttributeView = Files.getFileAttributeView(path, CloudFileAttributesView.class);
		
		if (cloudFileAttributeView == null) {
			LOG.warn("Could not get {} attribute view from path {}", CloudFileAttributesView.class, path);
			return false;
		}

		CloudAclFileAttributes aclFileAttributes;
		try {
			aclFileAttributes = cloudFileAttributeView.readAttributes();
		} catch (IOException e) {
			LOG.warn("Could not read ACL file attributes for {}", path);
			return false;
		}
		
		return checkAccessAllowed(aclFileAttributes.getAclSet(), userOrGroup, checkPermissions);
	}

	/**
	 * Checks the access for the given user/group such that the <em>all</em> of the permissions in
	 * <em>checkPermissions</em> are allowed in the <em>cloudAclEntrySet</em>
	 * 
	 * @param cloudAclEntrySet		Asset ACL permissions
	 * @param userOrGroup	A {@link UserPrincipal} or {@link GroupPrincipal}, which can be null for
	 * 								anonymous
	 * @param checkPermissions		Permissions to check for
	 * @return true if access is allowed with the given permissions, false otherwise
	 */
	@Override
	public boolean checkAccessAllowed(CloudAclEntrySet assetPermissions, UserPrincipal userOrGroup,
			Set<AclEntryPermission> checkPermissions) {
		boolean isGroup = userOrGroup != null ? GroupPrincipal.class.isAssignableFrom(userOrGroup.getClass()) : false;

		// Get a user's groups if required
		Set<GroupPrincipal> userGroups;
		if (!isGroup && userPrincipalLookupService != null && userOrGroup != null) {
			userGroups = userPrincipalLookupService.getUserPrincipalGroups(userOrGroup);
		} else {
			userGroups = null;
		}

		return doesUserHaveAccess(assetPermissions, userOrGroup, userGroups, checkPermissions);
	}

	/**
	 * Calculates if the user/group has access to this resource.
	 * @param assetPermissions 		Permissions granted on the asset
	 * @param userOrGroup			A user or group or null to check against an anonymous user/group
	 * @param userGroups			If the <em>currentUserOrGroup</em> is a {@link UserPrincipal} then these are the groups
	 * 								that the user also belongs to
	 * @param permissions 			Permission(s) to check
	 * 
	 * @return true if the user/group has access based on this ACL set, false otherwise
	 * 
	 * @throws IOException If invoking {@link #readAclFileAttributes()} throws an exception
	 */
	protected boolean doesUserHaveAccess(CloudAclEntrySet assetPermissions, UserPrincipal userOrGroup,
			Set<GroupPrincipal> userGroups, Set<AclEntryPermission> permissions) {
		boolean isGroup = userOrGroup != null ? GroupPrincipal.class.isAssignableFrom(userOrGroup.getClass()) : false;
		IsAllowed groupIsAllowed = IsAllowed.UNSET;
		IsAllowed userIsAllowed = IsAllowed.UNSET;
		IsAllowed userGroupIsAllowed = IsAllowed.UNSET;
		IsAllowed anonymousUserIsAllowed = IsAllowed.UNSET;
		IsAllowed anonymousGroupIsAllowed = IsAllowed.UNSET;

		// Iterate through the ACL set
		for(Iterator<CloudAclEntry<?>> i = assetPermissions.iterator(); i.hasNext(); ) {
			CloudAclEntry<?> aclCandidate = i.next();

			// Test for any matching permissions in the set
			if (aclCandidate.getPermissions().stream().anyMatch(p -> permissions.contains(p))) {
				// Is the candidate we are testing a group?
				boolean candidatePrincipalIsGroup = GroupPrincipal.class.isAssignableFrom(aclCandidate.getPrincipalClass());
				IsAllowed isAllowOrDeny =
						AclEntryType.ALLOW.equals(aclCandidate.getType()) ? IsAllowed.TRUE :
							(AclEntryType.DENY.equals(aclCandidate.getType()) ? IsAllowed.FALSE : IsAllowed.UNSET);
	
				if (userOrGroup != null) {
					boolean principalsEquals = aclCandidate.getPrincipal().equals(userOrGroup);
	
					// Are principals equal?
					if (principalsEquals) {
						// Is this a group principal?
						if (candidatePrincipalIsGroup) {
							if (groupIsAllowed != IsAllowed.UNSET) {
								LOG.warn("Conflicting Allow/Deny ACL's found for group ACL {} in ACL set: {}",
										aclCandidate, assetPermissions);
							}
	
							groupIsAllowed = isAllowOrDeny;
						} else {
							if (userIsAllowed != IsAllowed.UNSET) {
								LOG.warn("Conflicting Allow/Deny ACL's found for user ACL {} in ACL set: {}",
										aclCandidate, assetPermissions);
							}
	
							userIsAllowed = isAllowOrDeny;
						}
					} else {
						// This is a user so check the user's groups, only if another of the user's groups hasn't been
						// allowed before
						if (userGroupIsAllowed != IsAllowed.TRUE && candidatePrincipalIsGroup &&
								!isGroup && userGroups != null && !userGroups.isEmpty()) {
							if (userGroups.stream().anyMatch(g -> g.equals(aclCandidate.getPrincipal()))) {
								userGroupIsAllowed = isAllowOrDeny;
							}
						} else {
							// Assign allow/deny for the anonymous user/group
							if (AnonymousUserPrincipal.class.isAssignableFrom(aclCandidate.getPrincipal().getClass())) {
								anonymousUserIsAllowed = isAllowOrDeny;
							} else if (AnonymousGroupPrincipal.class.isAssignableFrom(aclCandidate.getPrincipal().getClass())) {
								anonymousGroupIsAllowed = isAllowOrDeny;
							}
						}
					}
	
				} else {
					// No user passed in
					// Assign allow/deny for the anonymous user/group
					if (AnonymousUserPrincipal.class.isAssignableFrom(aclCandidate.getPrincipal().getClass())) {
						anonymousUserIsAllowed = isAllowOrDeny;
					} else if (AnonymousGroupPrincipal.class.isAssignableFrom(aclCandidate.getPrincipal().getClass())) {
						anonymousGroupIsAllowed = isAllowOrDeny;
					}
				}
			}
		}

		// Debug log for the permissions
		if (LOG.isDebugEnabled()) {
			LOG.debug("ACL permissions for {} with permissions {} are [user={}, group={}, user-group={}, "
					+ "anonymous-user={}, anonymous-group={}] for ACL set: {}", userOrGroup, permissions,
					userIsAllowed, groupIsAllowed, userGroupIsAllowed, anonymousUserIsAllowed, anonymousGroupIsAllowed, assetPermissions);
		}

		return userIsAllowed == IsAllowed.TRUE ||
				(userIsAllowed != IsAllowed.FALSE && groupIsAllowed == IsAllowed.TRUE) ||
				(userIsAllowed != IsAllowed.FALSE && groupIsAllowed != IsAllowed.FALSE && userGroupIsAllowed == IsAllowed.TRUE) ||
				(userIsAllowed != IsAllowed.FALSE && groupIsAllowed != IsAllowed.FALSE && userGroupIsAllowed != IsAllowed.FALSE && anonymousUserIsAllowed == IsAllowed.TRUE) ||
				(userIsAllowed != IsAllowed.FALSE && groupIsAllowed != IsAllowed.FALSE && userGroupIsAllowed != IsAllowed.FALSE && anonymousUserIsAllowed != IsAllowed.FALSE && anonymousGroupIsAllowed == IsAllowed.TRUE);
	}

}
