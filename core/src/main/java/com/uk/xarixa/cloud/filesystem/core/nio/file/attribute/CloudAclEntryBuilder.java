package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.security.Principal;
import java.util.Arrays;
import java.util.EnumSet;

import com.uk.xarixa.cloud.filesystem.core.utils.SafeCloneable;

public class CloudAclEntryBuilder<T extends Principal> implements SafeCloneable<CloudAclEntryBuilder<T>> {
	private final CloudAclEntry<T> cloudAclEntry;

	public CloudAclEntryBuilder(Class<T> principalClass) {
		cloudAclEntry = new CloudAclEntry<T>(principalClass);
	}

	public CloudAclEntryBuilder<T> addFlag(AclEntryFlag flag) {
		cloudAclEntry.getFlags().add(flag);
		return this;
	}

	public CloudAclEntryBuilder<T> addFlags(AclEntryFlag... flags) {
		Arrays.stream(flags).forEach(f -> addFlag(f));
		return this;
	}

	public CloudAclEntryBuilder<T> addFlags(EnumSet<AclEntryFlag> flags) {
		flags.forEach(f -> addFlag(f));
		return this;
	}

	public CloudAclEntryBuilder<T> addPermission(AclEntryPermission permission) {
		cloudAclEntry.getPermissions().add(permission);
		return this;
	}

	public CloudAclEntryBuilder<T> addPermissions(AclEntryPermission... permissions) {
		Arrays.stream(permissions).forEach(p -> addPermission(p));
		return this;
	}

	public CloudAclEntryBuilder<T> addPermissions(EnumSet<AclEntryPermission> permissions) {
		permissions.stream().forEach(p -> addPermission(p));
		return this;
	}

	public CloudAclEntryBuilder<T> setPrincipal(T principal) {
		cloudAclEntry.setPrincipal(principal);
		return this;
	}
	
	public CloudAclEntryBuilder<T> setType(AclEntryType type) {
		cloudAclEntry.setType(type);
		return this;
	}

	public CloudAclEntry<T> build() {
		return cloudAclEntry;
	}
	
	@Override
	public CloudAclEntryBuilder<T> clone() {
		return new CloudAclEntryBuilder<T>(cloudAclEntry.getPrincipalClass())
				.addFlags(cloudAclEntry.getFlags())
				.addPermissions(cloudAclEntry.getPermissions())
				.setType(cloudAclEntry.getType())
				.setPrincipal(cloudAclEntry.getPrincipal());
	}

}