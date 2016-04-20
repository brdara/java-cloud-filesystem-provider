package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;

public interface AclConstants {
	static final UserPrincipal[] USER_PRINCIPAL_EMPTY_ARRAY = new UserPrincipal[0];

	EnumSet<AclEntryPermission> ALL_FILE_WRITE_PERMISSIONS =
			EnumSet.of(AclEntryPermission.WRITE_ACL, AclEntryPermission.WRITE_ATTRIBUTES, AclEntryPermission.WRITE_DATA,
					AclEntryPermission.WRITE_NAMED_ATTRS, AclEntryPermission.WRITE_OWNER, AclEntryPermission.APPEND_DATA);
	EnumSet<AclEntryPermission> ALL_DIRECTORY_WRITE_PERMISSIONS =
			EnumSet.of(AclEntryPermission.WRITE_ACL, AclEntryPermission.WRITE_ATTRIBUTES, AclEntryPermission.WRITE_DATA,
					AclEntryPermission.WRITE_NAMED_ATTRS, AclEntryPermission.WRITE_OWNER, AclEntryPermission.APPEND_DATA,
					AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.DELETE,
					AclEntryPermission.DELETE_CHILD);

	EnumSet<AclEntryPermission> ALL_FILE_READ_PERMISSIONS =
			EnumSet.of(AclEntryPermission.READ_ACL, AclEntryPermission.READ_ATTRIBUTES, AclEntryPermission.READ_DATA,
					AclEntryPermission.READ_NAMED_ATTRS);
	EnumSet<AclEntryPermission> ALL_DIRECTORY_READ_PERMISSIONS =
			EnumSet.of(AclEntryPermission.READ_ACL, AclEntryPermission.READ_ATTRIBUTES, AclEntryPermission.READ_DATA,
					AclEntryPermission.READ_NAMED_ATTRS, AclEntryPermission.LIST_DIRECTORY);

	EnumSet<AclEntryPermission> ALL_FILE_EXEC_PERMISSIONS = EnumSet.of(AclEntryPermission.EXECUTE);

	EnumSet<AclEntryPermission> READ_ACL_PERMISSIONS = EnumSet.of(AclEntryPermission.READ_ACL);
	EnumSet<AclEntryPermission> WRITE_ACL_PERMISSIONS = EnumSet.of(AclEntryPermission.WRITE_ACL);
	EnumSet<AclEntryPermission> READ_WRITE_ACL_PERMISSIONS = EnumSet.of(AclEntryPermission.READ_ACL, AclEntryPermission.WRITE_ACL);

}
