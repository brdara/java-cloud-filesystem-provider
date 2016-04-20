package com.uk.xarixa.cloud.filesystem.core.security;

import java.nio.file.attribute.UserPrincipal;

/**
 * A class which defines every user including anonymous users. This is useful when defining ACL's.
 * @see AnonymousGroupPrincipal
 */
public class AnonymousUserPrincipal implements UserPrincipal, Comparable<UserPrincipal> {
	public final static AnonymousUserPrincipal INSTANCE = new AnonymousUserPrincipal();
	private final static String NAME = "Anonymous User";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public int compareTo(UserPrincipal userPrincipal) {
		return AnonymousUserPrincipal.class.isAssignableFrom(userPrincipal.getClass()) ? 0 : -1;
	}

	@Override
	public boolean equals(Object obj) {
		return AnonymousUserPrincipal.class.isAssignableFrom(obj.getClass());
	}

	@Override
	public int hashCode() {
		return AnonymousUserPrincipal.class.hashCode();
	}

}
