package com.uk.xarixa.cloud.filesystem.core.security;

import java.nio.file.attribute.GroupPrincipal;
import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.GroupMembershipPrincipal;

/**
 * A simple placeholder group principal which defines all groups including anonymous. A user or group is
 * always a member of this group by default. This is useful when defining ACL's.
 * @see AnonymousUserPrincipal
 */
public class AnonymousGroupPrincipal extends GroupMembershipPrincipal implements Comparable<GroupPrincipal> {
	public final static AnonymousGroupPrincipal INSTANCE = new AnonymousGroupPrincipal();
	private final static String NAME = "Anonymous Group";
	private final static Set<? extends Principal> members =
			Collections.unmodifiableSet(Sets.newHashSet(AnonymousUserPrincipal.INSTANCE));

	@Override
	public String getName() {
		return NAME;
	}

	/**
	 * Always returns true in this implementation
	 */
	@Override
	public boolean addMember(Principal user) {
		return true;
	}

	/**
	 * Always returns false in this implementation
	 */
	@Override
	public boolean removeMember(Principal user) {
		return false;
	}

	/**
	 * Always returns true in this implementation
	 */
	@Override
	public boolean isMember(Principal member) {
		return true;
	}

	/**
	 * Returns all users
	 */
	@Override
	public Enumeration<? extends Principal> members() {
		Iterator<? extends Principal> allMembers = members.iterator();

		return new Enumeration<Principal>() {
			@Override
			public boolean hasMoreElements() {
				return allMembers.hasNext();
			}

			@Override
			public Principal nextElement() {
				return allMembers.next();
			}
		};
	}

	@Override
	public int compareTo(GroupPrincipal groupPrincipal) {
		return AnonymousGroupPrincipal.class.isAssignableFrom(groupPrincipal.getClass()) ? 0 : -1;
	}

	@Override
	public boolean equals(Object obj) {
		return AnonymousGroupPrincipal.class.isAssignableFrom(obj.getClass());
	}

	@Override
	public int hashCode() {
		return AnonymousGroupPrincipal.class.hashCode();
	}

}
