package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.SetUtils;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

public class CloudFileAttributesView implements BasicFileAttributeView {
	public static final String VIEW_NAME = "cloudFileAttributesView";
	public static final Set<AclEntryPermission> READ_ACL_PERMS =
			EnumSet.of(AclEntryPermission.READ_ATTRIBUTES, AclEntryPermission.READ_ATTRIBUTES);
	public static final Set<AclEntryPermission> WRITE_ACL_PERMS = EnumSet.of(AclEntryPermission.WRITE_ACL);
	private static final Logger LOG = LoggerFactory.getLogger(CloudFileAttributesView.class);
	private final BlobStoreContext context;
	private final CloudPath path;

	public CloudFileAttributesView(BlobStoreContext context, CloudPath path) {
		this.context = context;
		this.path = path;
	}

	@Override
	public String name() {
		return VIEW_NAME;
	}

	/**
	 * Reads all of the basic cloud file attributes. Access is not checked here.
	 * @return
	 * @throws IOException
	 */
	public CloudAclFileAttributes readAttributes() throws IOException {
		return checkAccess(READ_ACL_PERMS);
	}

	protected boolean isContainer() {
		// Is it a container or a directory?
		if (path.getPathName() == null && context.getBlobStore().containerExists(path.getContainerName())) {
			return true;
		}
		
		return false;
	}

	protected boolean isDirectory() {
		if (context.getBlobStore().directoryExists(path.getContainerName(), path.getPathName())) {
			return true;
		}

		return false;
	}

	/**
	 * <p>
	 * This first invokes {@link #readInternalAclFileAttributes()}
	 * to determine file existence, which throws a {@link FileNotFoundException} if the file doesn't exist.
	 * Access to the file is then checked by first retrieving the
	 * {@link CloudHostConfiguration#getUserGroupLookupService() user service} and if it is
	 * of has the mixin interface {@link UserGroupLookupService} then it will get the
	 * {@link CloudHostConfiguration#getCloudHostSecurityManager() cloud host security manager} and invoke
	 * {@link CloudHostSecurityManager#checkAccessAllowed(CloudAclEntrySet, UserPrincipal, Set)} to work out
	 * if the permissions are valid.
	 * </p>
	 * <p>
	 * If no {@link UserGroupLookupService} exists or the lookup returns <em>null</em> then the
	 * {@link AnonymousUserPrincipal#INSTANCE} is used.
	 * </p>
	 * <p>
	 * If no {@link CloudHostSecurityManager} is available then access will just be allowed as no access
	 * check is possible.
	 * </p>
	 * @throws SecurityException If access is not allowed
	 */
	public CloudAclFileAttributes checkAccess(Set<AclEntryPermission> checkPermissions) throws IOException {
		// Throws FileNotFoundException
		CloudAclFileAttributes readAttributes = readInternalAclFileAttributes();
		
		// Read the config
		CloudHostConfiguration cloudHostConfiguration = path.getFileSystem().getCloudHostConfiguration();

		// Get the security manager
		CloudHostSecurityManager cloudHostSecurityManager = cloudHostConfiguration.getCloudHostSecurityManager();
		if (cloudHostSecurityManager == null) {
			// No security manager, no access
			LOG.info("No {} found in cloud host configuration {}, default action is to allow all access",
					CloudHostSecurityManager.class, cloudHostConfiguration);
			return readAttributes;
		}

		// Try to get the current user
		UserPrincipal currentUser = null;
		UserGroupLookupService<?> userGroupLookupService = cloudHostConfiguration.getUserGroupLookupService();
		if (userGroupLookupService != null) {
			currentUser = ((UserGroupLookupService<?>)userGroupLookupService).getCurrentUser();
		}
		
		// Default to anonymous
		if (currentUser == null) {
			currentUser = AnonymousUserPrincipal.INSTANCE;
		}

		// Check for access against the ACL's
		if (!cloudHostSecurityManager.checkAccessAllowed(readAttributes.getAclSet(), currentUser, checkPermissions)) {
			LOG.debug("Permission doesn't allow access for '{}': {}", path.toString(), checkPermissions);
			throw new AccessDeniedException(path.toString(), null, "Permission doesn't allow access");
		}
		
		return readAttributes;
	}
	
	/**
	 * Reads all of the basic + ACL file attributes. Access is not checked here.
	 * @return
	 * @throws IOException
	 */
	protected CloudAclFileAttributes readInternalAclFileAttributes() throws IOException {
		if (isContainer() || isDirectory()) {
			return new CloudAclFileAttributes();
		}

		try {
			BlobMetadata blobMetadata = context.getBlobStore().blobMetadata(path.getContainerName(), path.getPathName());
			BlobAccess blobAccess = context.getBlobStore().getBlobAccess(path.getContainerName(), path.getPathName());
			return new CloudAclFileAttributes(blobMetadata, blobAccess);
		} catch (KeyNotFoundException k) {
			if (context.getBlobStore().directoryExists(path.getContainerName(), path.getPathName())) {
				return new CloudAclFileAttributes();
			}
			throw new FileNotFoundException("Unable to locate the file '" + path.getContainerName() + "|" + path.getPathName() + "'");
		}
	}

	/**
	 * Sets ACL attributes for the file. Only {@link PublicPrivateCloudPermissionsPrincipal} is accepted in the ACL
	 * permissions set by this class. Extend it to allow for storage of other ACL's. The set is first
	 * {@link CloudAclEntrySet#optimise() optimised} before being set.
	 * @param	cloudAclEntrySet	The ACL entry set for this file
	 * @return	The ACL entries which could not be set.
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public Set<CloudAclEntry<?>> setAclFileAttributes(CloudAclEntrySet cloudAclEntrySet) throws IOException {
		checkAccess(WRITE_ACL_PERMS);

		// Optimise the entry set first
		cloudAclEntrySet.optimise();

		Set<CloudAclEntry<?>> validPrincipals =
			cloudAclEntrySet.stream().filter(e -> PublicPrivateCloudPermissionsPrincipal.class.isAssignableFrom(e.getPrincipalClass()))
				.collect(Collectors.toSet());
		// TODO: Reverse PublicPrivateCloudPermissionsPrincipal if the permission is DENY
		// TODO: Maybe add validation methods on the entry and principal?
		validPrincipals.forEach(pe -> context.getBlobStore().setBlobAccess(path.getContainerName(), path.getPathName(),
					((PublicPrivateCloudPermissionsPrincipal)pe.getPrincipal()).getBlobAccess()));

		// If we got only a subset of the principals then log the invalid ones
		if (validPrincipals.size() < cloudAclEntrySet.size()) {
			Set<CloudAclEntry<?>> differences = SetUtils.difference(cloudAclEntrySet.getAclEntries(), validPrincipals);

			if (!differences.isEmpty()) {
				LOG.warn("Skipping unknown ACL's from the set: {}", differences);
				return differences;
			}
		}

		return Collections.EMPTY_SET;
	}

	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
		throw new UnsupportedOperationException();
	}

}
