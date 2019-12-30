package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.security.Principal;
import java.util.Set;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobMetadata;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.NotOwnerException;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;

/**
 * <p>
 * Cloud ACL's as file attributes. These ACL's are set to {@link AnonymousUserPrincipal} access.
 * Implementors should override this behaviour to allow for the ACL's to be created and stored
 * with proper owners. No storage is provided in this implementation for ACL's.
 * </p>
 * <p>
 * The {@link CloudAclEntrySet#getOwners() owners of the ACL} are by default owners of the file.
 * </p>
 */
public class CloudAclFileAttributes extends CloudBasicFileAttributes {
	public static final String VIEW_NAME = "cloud:cloudAclFileAttributesView";
	private CloudAclEntrySet aclSet;

	/**
	 * Creates an instance backed by a {@link CloudAclEntrySet} initialised with a {@link DefaultCloudAclEntryConflictChecker}
	 */
	public CloudAclFileAttributes() {
		this(DefaultCloudAclEntryConflictChecker.INSTANCE);
	}
	
	/**
	 * Creates an instance backed by a {@link CloudAclEntrySet} with the specified conflict checker
	 * @param conflictChecker
	 */
	public CloudAclFileAttributes(CloudAclEntryConflictChecker conflictChecker) {
		super();
		aclSet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, conflictChecker);
	}

	/**
	 * Creates an instance backed by a {@link CloudAclEntrySet} initialised with a {@link DefaultCloudAclEntryConflictChecker}
	 * @see #CloudAclFileAttributes(CloudAclEntryConflictChecker, BlobMetadata, BlobAccess)
	 */
	public CloudAclFileAttributes(BlobMetadata blobMetadata, BlobAccess blobAccess) {
		this(DefaultCloudAclEntryConflictChecker.INSTANCE, blobMetadata, blobAccess);
	}

	/**
	 * <p>
	 * This constructor can only discern some very basic permissions. It assumes that because you have access
	 * to this container then you have all CRUD operation access. This may not be true. More sophisticated
	 * implementations should be able to tell the exact permissions.
	 * </p>
	 * <p>
	 * There is a lone {@link CloudAclEntry} created which has the default permissions. It is of type
	 * {@link PublicPrivateCloudPermissionsPrincipal}.
	 * </p>
	 * <p>
	 * Subclasses may implement different permissions.
	 * </p>
	 */
	public CloudAclFileAttributes(CloudAclEntryConflictChecker conflictChecker, BlobMetadata blobMetadata, BlobAccess blobAccess) {
		super(blobMetadata);
		aclSet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, conflictChecker);
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> entry =
			new CloudAclEntryBuilder<>(PublicPrivateCloudPermissionsPrincipal.class)
				.addPermissions(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA, AclEntryPermission.APPEND_DATA,
						AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY,
						AclEntryPermission.DELETE, AclEntryPermission.DELETE_CHILD, AclEntryPermission.LIST_DIRECTORY,
						AclEntryPermission.READ_ACL, AclEntryPermission.WRITE_ACL,
						AclEntryPermission.READ_ATTRIBUTES, AclEntryPermission.READ_ATTRIBUTES)
				.setType(AclEntryType.ALLOW)
				.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(blobAccess))
				.build();
		addAcl(AnonymousUserPrincipal.INSTANCE, entry);
	}

	public String name() {
		return VIEW_NAME;
	}

	/**
	 * Retrieves the ACL's
	 * @return A set, may be empty
	 */
	public CloudAclEntrySet getAclSet() {
		return aclSet;
	}

	/**
	 * Sets all of the ACL's
	 * @return
	 */
	public void setAclSet(Principal caller, CloudAclEntrySet acl) {
		if (aclSet != null && !aclSet.isOwner(caller)) {
			throw new RuntimeException("Cannot add ACL for " + caller.getName() +
					", the user is not one of the owners of the original ACL");
		}

		aclSet = acl;
	}

	/**
	 * Adds the ACL to the current set of ACL's
	 * @param acl true if the ACL was added, false if it already exists in the set
	 * @see CloudAclEntrySet#add(CloudAclEntry)
	 */
	public boolean addAcl(Principal caller, CloudAclEntry<?> acl) {
		return addAcl(caller, acl, false);
	}

	/**
	 * Adds the ACL to the current set of ACL's
	 * @param acl true if the ACL was added, false if it already exists in the set and <em>force</em> was set to false
	 * @param force true if you would like to add this ACL and remove any conflicting ACL, false otherwise
	 * @see CloudAclEntrySet#addAclEntry(CloudAclEntry, boolean)
	 */
	public boolean addAcl(Principal caller, CloudAclEntry<?> acl, boolean force) {
		Set<CloudAclEntry<?>> conflicts;

		try {
			conflicts = aclSet.addAclEntry(caller, acl, force);
		} catch (NotOwnerException e) {
			throw new RuntimeException("Cannot add ACL for " + caller.getName() +
					", the user is not one of the owners of the ACL", e);
		}

		return force ? true : conflicts.isEmpty();
	}

	/**
	 * Gets the ACL owners through {@link CloudAclEntrySet#getOwners()}
	 * @return
	 */
	public Set<Principal> getOwners() {
		return aclSet.getOwners();
	}

}
