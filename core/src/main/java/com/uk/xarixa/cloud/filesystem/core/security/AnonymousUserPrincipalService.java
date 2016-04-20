package com.uk.xarixa.cloud.filesystem.core.security;

import java.io.IOException;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * A default implementation of {@link UserGroupLookupService} which recognises
 * anonymous access through {@link AnonymousUserPrincipal} and {@link AnonymousGroupPrincipal}
 */
public class AnonymousUserPrincipalService extends UserPrincipalLookupService implements UserGroupLookupService<String> {
	private final static Set<UserPrincipal> allUsers = Collections.unmodifiableSet(Sets.newHashSet(AnonymousUserPrincipal.INSTANCE));
	private final static Set<GroupPrincipal> allGroups = Collections.unmodifiableSet(Sets.newHashSet(AnonymousGroupPrincipal.INSTANCE));

	/**
	 * Always returns {@link AnonymousUserPrincipal#INSTANCE}
	 */
	@Override
	public UserPrincipal getCurrentUser() {
		return AnonymousUserPrincipal.INSTANCE;
	}

	@Override
	public UserPrincipal lookupPrincipalByUserId(String identifier) {
		if (AnonymousUserPrincipal.INSTANCE.getName().equals(identifier.toString())) {
			return AnonymousUserPrincipal.INSTANCE;
		}

		return null;
	}

	@Override
	public GroupPrincipal lookupPrincipalByGroupId(String identifier) {
		if (AnonymousGroupPrincipal.INSTANCE.getName().equals(identifier.toString())) {
			return AnonymousGroupPrincipal.INSTANCE;
		}

		return null;
	}

	@Override
	public Iterable<UserPrincipal> getAllUsers() {
		return allUsers;
	}

	@Override
	public Iterable<GroupPrincipal> getAllGroups() {
		return allGroups;
	}

	@Override
	public Set<GroupPrincipal> getUserPrincipalGroups(UserPrincipal userPrincipal) {
		return AnonymousUserPrincipal.INSTANCE.equals(userPrincipal) ? allGroups : Collections.emptySet();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<UserPrincipal, Set<GroupPrincipal>> getUserPrincipalsGroups(UserPrincipal... userPrincipals) {
		if (Arrays.stream(userPrincipals).anyMatch(p -> AnonymousUserPrincipal.INSTANCE.equals(p))) {
			Map<UserPrincipal, Set<GroupPrincipal>> map = new HashMap<>();
			map.put(AnonymousUserPrincipal.INSTANCE, allGroups);
		}
		
		return Collections.EMPTY_MAP;
	}

	/**
	 * Always throws {@link UnsupportedOperationException}
	 */
	@Override
	public void setUserPrincipalGroups(UserPrincipal userPrincipal, Set<GroupPrincipal> groupPrincipal)
			throws SecurityException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Always throws {@link UnsupportedOperationException}
	 */
	@Override
	public void setUserPrincipalGroups(Map<UserPrincipal, Set<GroupPrincipal>> userGroupPrincipalsMap)
			throws SecurityException {
		throw new UnsupportedOperationException();
	}

	@Override
	public UserPrincipal lookupPrincipalByName(String name) throws IOException {
		return lookupPrincipalByUserId(name);
	}

	@Override
	public GroupPrincipal lookupPrincipalByGroupName(String group) throws IOException {
		return lookupPrincipalByGroupId(group);
	}

}
