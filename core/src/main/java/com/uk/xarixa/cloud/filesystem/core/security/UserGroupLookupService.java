package com.uk.xarixa.cloud.filesystem.core.security;

import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.util.Map;
import java.util.Set;

import com.uk.xarixa.cloud.filesystem.core.file.attribute.CreatorFileAttribute;

/**
 * A user lookup service. The existing Java one is a bit limited for our purposes.
 */
public interface UserGroupLookupService<ID extends Object> {

	/**
	 * Gets the current user
	 * @return
	 */
	UserPrincipal getCurrentUser();

	/**
	 * Looks up a user principal by an identifier
	 * @param identifier
	 * @return
	 * @see CreatorFileAttribute
	 */
	UserPrincipal lookupPrincipalByUserId(ID identifier);

	/**
	 * Looks up a group principal by an identifier
	 * @param identifier
	 * @return
	 * @see CreatorFileAttribute
	 */
	GroupPrincipal lookupPrincipalByGroupId(ID identifier);

	/**
	 * Gets all of the users
	 */
	Iterable<? extends UserPrincipal> getAllUsers();

	/**
	 * Gets all of the groups
	 */
	Iterable<? extends GroupPrincipal> getAllGroups();

	/**
	 * Gets the groups which this user belongs to
	 * @param userPrincipal
	 * @return
	 */
	Set<GroupPrincipal> getUserPrincipalGroups(UserPrincipal userPrincipal);

	/**
	 * Gets the groups which these users belong to
	 * @param userPrincipal
	 * @return
	 */
	Map<UserPrincipal,Set<GroupPrincipal>> getUserPrincipalsGroups(UserPrincipal... userPrincipal);

	/**
	 * Gets the groups which this user belongs to
	 * @param userPrincipal
	 * @throws SecurityException If the user cannot become a member of all of these groups for any reason.
	 */
	void setUserPrincipalGroups(UserPrincipal userPrincipal, Set<GroupPrincipal> groupPrincipal)
		throws SecurityException;

	/**
	 * Sets the groups which these users belongs to
	 * @param userPrincipal
	 * @throws SecurityException If the user cannot become a member of all of these groups for any reason.
	 */
	void setUserPrincipalGroups(Map<UserPrincipal,Set<GroupPrincipal>> userGroupPrincipalsMap)
		throws SecurityException;

}
