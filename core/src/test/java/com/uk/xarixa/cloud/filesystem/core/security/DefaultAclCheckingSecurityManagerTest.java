package com.uk.xarixa.cloud.filesystem.core.security;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.Set;

import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.TestGroupImpl;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.TestUserImpl;

@RunWith(BlockJUnit4ClassRunner.class)
public class DefaultAclCheckingSecurityManagerTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	private DefaultAclCheckingSecurityManager mgr;
	private UserGroupLookupService<?> userPrincipalLookupService;

	@Before
	public void setUp() {
		userPrincipalLookupService = context.mock(UserGroupLookupService.class);
		mgr = new DefaultAclCheckingSecurityManager(userPrincipalLookupService);
	}

	@Test
	public void testDoesUserHaveAccessFailsForAUserPrincipalWithNoRules() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, user, null, EnumSet.of(AclEntryPermission.ADD_FILE)));

		// Now try it with some groups as well
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");
		Set<GroupPrincipal> userGroups = Sets.newHashSet(group1, group2);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, user, userGroups, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessFailsForAnAnonymousUserWithNoRules() {
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, null, null, EnumSet.of(AclEntryPermission.ADD_FILE)));

		// Now try it with some groups as well to try and break it
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");
		Set<GroupPrincipal> userGroups = Sets.newHashSet(group1, group2);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, null, userGroups, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAUserPrincipalWithAnAllowRule() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<UserPrincipal> entry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(user)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAUserPrincipalWithAnAnonymousAllowRule() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<UserPrincipal> entry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(new AnonymousUserPrincipal())
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAnAnonymousUserWithAnAnonymousAllowRule() {
		CloudAclEntry<UserPrincipal> entry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(new AnonymousUserPrincipal())
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, null, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAUserWithAnAnonymousGroupAllowRule() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(AnonymousGroupPrincipal.INSTANCE)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAnAnonymousUserWithAnAnonymousGroupAllowRule() {
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(AnonymousGroupPrincipal.INSTANCE)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, null, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessFailsForAUserWithAnAnonymousGroupAllowRuleButAnonymousUserDenyRule() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(AnonymousGroupPrincipal.INSTANCE)
			.build();
		CloudAclEntry<UserPrincipal> entry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.addPermission(AclEntryPermission.ADD_FILE)
				.setType(AclEntryType.DENY)
				.setPrincipal(new AnonymousUserPrincipal())
				.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1, entry2);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, user, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessFailsForAUserGroupWithAnAllowRuleAndAUserWithADenyRule() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<UserPrincipal> entry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.DENY)
			.setPrincipal(user)
			.build();
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");
		Set<GroupPrincipal> userGroups = Sets.newHashSet(group1, group2);
		CloudAclEntry<GroupPrincipal> entry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.addPermission(AclEntryPermission.ADD_FILE)
				.setType(AclEntryType.ALLOW)
				.setPrincipal(group2)
				.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1, entry2);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, user, userGroups, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAUsersGroupWithAnAllowRule() {
		UserPrincipal user = new TestUserImpl("user1");
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");
		Set<GroupPrincipal> userGroups = Sets.newHashSet(group1, group2);
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(group2)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, userGroups, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAGroupWithAnAllowRule() {
		GroupPrincipal group = new TestGroupImpl("group1");
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(group)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, group, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAGroupWithAnAnonymousGroupAllowRule() {
		GroupPrincipal group = new TestGroupImpl("group1");
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(AnonymousGroupPrincipal.INSTANCE)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, group, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessFailsForAUserPrincipalWithADenyRule() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<UserPrincipal> entry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.DENY)
			.setPrincipal(user)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, user, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessFailsForAUsersGroupWithADenyRule() {
		UserPrincipal user = new TestUserImpl("user1");
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");
		Set<GroupPrincipal> userGroups = Sets.newHashSet(group1, group2);
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.DENY)
			.setPrincipal(group2)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, user, userGroups, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessFailsForAGroupWithADenyRule() {
		GroupPrincipal group = new TestGroupImpl("group1");
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermission(AclEntryPermission.ADD_FILE)
			.setType(AclEntryType.DENY)
			.setPrincipal(group)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertFalse(mgr.doesUserHaveAccess(assetPermissions, group, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAUserPrincipalWithMultipleCheckPermissionsAndMultipleAssetPermissionsAllowed() {
		UserPrincipal user = new TestUserImpl("user1");
		CloudAclEntry<UserPrincipal> entry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(user)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, null,
				EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.WRITE_ACL)));
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, null,
				EnumSet.of(AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)));
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, null,
				EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForUsersGroupWithMultipleCheckPermissionsAndMultipleAssetPermissionsAllowed() {
		UserPrincipal user = new TestUserImpl("user1");
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");
		Set<GroupPrincipal> userGroups = Sets.newHashSet(group1, group2);
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(group2)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, userGroups, EnumSet.of(AclEntryPermission.ADD_FILE)));
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, userGroups,
				EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.WRITE_ACL)));
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, user, userGroups,
				EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)));
	}

	@Test
	public void testDoesUserHaveAccessSucceedsForAGroupWithMultipleCheckPermissionsAndMultipleAssetPermissionsAllowed() {
		GroupPrincipal group = new TestGroupImpl("group1");
		CloudAclEntry<GroupPrincipal> entry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)
			.setType(AclEntryType.ALLOW)
			.setPrincipal(group)
			.build();
		CloudAclEntrySet assetPermissions = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, entry1);
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, group, null, EnumSet.of(AclEntryPermission.ADD_FILE)));
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, group, null,
				EnumSet.of(AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)));
		Assert.assertTrue(mgr.doesUserHaveAccess(assetPermissions, group, null,
				EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_ACL)));
	}

}
