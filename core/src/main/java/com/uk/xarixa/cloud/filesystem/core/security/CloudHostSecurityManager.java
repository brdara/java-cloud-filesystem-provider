package com.uk.xarixa.cloud.filesystem.core.security;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;

/**
 * An interface which implementors can create concrete subclasses from to implement security
 * mechanisms within the cloud file system. Because of the plethora of security managers
 * available it was decided that this would be the best way to provide a pluggable layer to the
 * security manager of choice. So an implementor can plugin whichever mechanism they choose
 * by adapting it to this implementation.
 */
public interface CloudHostSecurityManager {

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
	boolean checkAccessAllowed(CloudPath path, UserPrincipal userOrGroup,
			Set<AclEntryPermission> checkPermissions);

	/**
	 * As for {@link #checkAccessAllowed(CloudPath, UserPrincipal, Set)} but works on a set of asset
	 * permissions.
	 * @param assetPermissions
	 * @param userOrGroup
	 * @param checkPermissions
	 * @return
	 */
	boolean checkAccessAllowed(CloudAclEntrySet assetPermissions, UserPrincipal userOrGroup,
			Set<AclEntryPermission> checkPermissions);

}
