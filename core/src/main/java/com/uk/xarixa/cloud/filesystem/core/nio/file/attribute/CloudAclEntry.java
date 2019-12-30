package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.security.Principal;
import java.util.EnumSet;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.uk.xarixa.cloud.filesystem.core.utils.SafeCloneable;

/**
 * An ACL entry similar to {@link java.security.acl.AclEntry} but it can accept any principal
 */
public class CloudAclEntry<T extends Principal> implements Comparable<CloudAclEntry<T>>, SafeCloneable<CloudAclEntry<T>> {
	private final Class<T> principalClass;
	private T principal;
	private AclEntryType type;
	private EnumSet<AclEntryFlag> flags = EnumSet.noneOf(AclEntryFlag.class);
	private EnumSet<AclEntryPermission> permissions = EnumSet.noneOf(AclEntryPermission.class);

	/**
	 * @param principalClass Must implement {@link Comparable}
	 */
	public CloudAclEntry(Class<T> principalClass) {
		this.principalClass = principalClass;
	}

	@Override
	public int compareTo(CloudAclEntry<T> other) {
		return new CompareToBuilder()
			.append(principalClass.getName(), other.principalClass.getName())
			.append(principal, other.principal)
			.append(type, other.type)
			.append(flags.toArray(), other.flags.toArray())
			.append(permissions.toArray(), other.permissions.toArray())
			.toComparison();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (!(obj instanceof CloudAclEntry)) {
			return false;
		}

		CloudAclEntry<?> other = (CloudAclEntry<?>)obj;
		return new EqualsBuilder()
				.append(principalClass, other.principalClass)
				.append(principal, other.principal)
				.append(type, other.type)
				.append(flags, other.flags)
				.append(permissions, other.permissions)
				.isEquals();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(principalClass)
				.append(principal)
				.append(type)
				.append(flags)
				.append(permissions)
				.toHashCode();
	}
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	public EnumSet<AclEntryFlag> getFlags() {
		return flags;
	}

	public void setFlags(EnumSet<AclEntryFlag> flags) {
		this.flags = flags;
	}

	public EnumSet<AclEntryPermission> getPermissions() {
		return permissions;
	}

	public void setPermissions(EnumSet<AclEntryPermission> permissions) {
		this.permissions = permissions;
	}

	public AclEntryType getType() {
		return type;
	}

	public void setType(AclEntryType type) {
		this.type = type;
	}

	public T getPrincipal() {
		return principal;
	}

	public void setPrincipal(T principal) {
		if (principal != null) {
			if (!principalClass.isAssignableFrom(principal.getClass())) {
				throw new IllegalArgumentException("Principal class " + principal.getClass() +
						" is not the same or a subclass of the declared principal class " + principalClass);
			}
	
			if (!Comparable.class.isAssignableFrom(principal.getClass())) {
				throw new IllegalArgumentException("The principal class " + principal.getClass() +
						" must implement " + Comparable.class.toString());
			}
		}

		this.principal = principal;
	}

	public Class<T> getPrincipalClass() {
		return principalClass;
	}

	@Override
	public CloudAclEntry<T> clone() {
		return new CloudAclEntryBuilder<T>(principalClass)
				.addFlags(flags)
				.addPermissions(permissions)
				.setType(type)
				.setPrincipal(principal)
				.build();
	}

}
