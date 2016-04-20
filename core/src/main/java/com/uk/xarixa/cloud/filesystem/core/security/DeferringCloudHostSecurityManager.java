package com.uk.xarixa.cloud.filesystem.core.security;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

/**
 * Essentially this security manager defers all security to the cloud, so if
 * the user connecting to the cloud allows an operation then it will be allowed by
 * default. With this security manager there is no point in implementing the
 * {@link CloudFileAttributesView#readAclFileAttributes()} beyond what is already there.
 */
public class DeferringCloudHostSecurityManager implements CloudHostSecurityManager {

	/**
	 * @return Always true in this implementation
	 */
	@Override
	public boolean checkAccessAllowed(CloudPath path, UserPrincipal userOrGroup,
			Set<AclEntryPermission> checkPermissions) {
		return true;
	}

	/**
	 * @return Always true in this implementation
	 */
	@Override
	public boolean checkAccessAllowed(CloudAclEntrySet assetPermissions, UserPrincipal userOrGroup,
			Set<AclEntryPermission> checkPermissions) {
		return true;
	}

}
