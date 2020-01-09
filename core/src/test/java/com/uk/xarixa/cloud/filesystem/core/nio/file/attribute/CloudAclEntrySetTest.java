package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.Arrays;
import java.util.Set;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.LastOwnerException;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.NotOwnerException;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousGroupPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudAclEntrySetTest {
    @Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};
	
//	public CloudAclEntrySetTest(Boolean useSecurityManager) {
//		if (useSecurityManager) {
//			// Set up Java policy and initialise a security manager to use a local policy file
//			String policyFile = System.getProperty("user.dir") +
//					StringUtils.replace("/src/test/resources/test-java.policy", "/", File.separator);
////			System.out.println("User dir: " + policyFile);
//			System.setProperty("java.security.policy", "file:" + policyFile);
//			System.setSecurityManager(new SecurityManager());
//		} else {
//			System.setSecurityManager(null);
//		}
//	}
	
	@Test
	public void testCreateCloudAclEntrySetWillNotCreateWithAnEmptyAclOwner() {
		try {
			new CloudAclEntrySet((Principal)null);
			Assert.fail("Should not be able to create a cloud ACL entry set without an owner");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}
	
	@Test
	public void testCreateCloudAclEntrySetWithAnAnonymousUserOwnerWillIdentifyAnyOwnershipOverTheSet() {
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		Assert.assertTrue(cloudAclEntrySet.isOwner(AnonymousUserPrincipal.INSTANCE));
		Assert.assertTrue(cloudAclEntrySet.hasAnonymousOwner());
	}

	@Test
	public void testCreateCloudAclEntrySetWithAnAnonymousUserGroupWillIdentifyAnyOwnershipOverTheSet() {
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(AnonymousGroupPrincipal.INSTANCE);
		Assert.assertTrue(cloudAclEntrySet.isOwner(AnonymousGroupPrincipal.INSTANCE));
		Assert.assertTrue(cloudAclEntrySet.hasAnonymousOwner());
	}

	@Test
	public void testCreateCloudAclEntrySetWithAnAnonymousUserOwnerWillAllowAnyOwnership() {
		TestUserImpl user1 = new TestUserImpl("user1");
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		Assert.assertTrue(cloudAclEntrySet.hasAnonymousOwner());
	}

	@Test
	public void testCreateCloudAclEntrySetWithAUserOwnerWillNotAllowAnonymousOwnership() {
		TestUserImpl user1 = new TestUserImpl("user1");
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(user1);
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		Assert.assertFalse(cloudAclEntrySet.isOwner(AnonymousUserPrincipal.INSTANCE));
		Assert.assertFalse(cloudAclEntrySet.isOwner(AnonymousGroupPrincipal.INSTANCE));
		Assert.assertFalse(cloudAclEntrySet.hasAnonymousOwner());
	}

	@Test
	public void testCreateCloudAclEntrySetWithAUserOwnerWillNotAllowTheLastOwnerToBeDeleted() {
		TestUserImpl user1 = new TestUserImpl("user1");
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(user1);
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		
		try {
			cloudAclEntrySet.deleteOwner(user1, user1);
			Assert.fail("Did not expect to be able to delete the last owner");
		} catch (LastOwnerException e) {
			// Cannot delete the last owner
		} catch (NotOwnerException e) {
			Assert.fail("Incorrect exception");
		}
	}

	@Test
	public void testCreateCloudAclEntrySetWithAUserOwnerWillNotAllowSomeoneWhoIsntAnOwnerToDeleteAnOwner() throws NotOwnerException, LastOwnerException {
		TestUserImpl user1 = new TestUserImpl("user1");
		TestUserImpl user2 = new TestUserImpl("user2");
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(Sets.newHashSet(user1, user2));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user2));
		
		// Delete user2 from the owners
		Assert.assertTrue(cloudAclEntrySet.deleteOwner(user1, user2));

		try {
			cloudAclEntrySet.deleteOwner(user2, user1);
			Assert.fail("Did not expect to be able to delete the owner using a caller who is not an owner");
		} catch (LastOwnerException e) {
			Assert.fail("Incorrect exception");
		} catch (NotOwnerException e) {
			// This is not the owner
		}
	}

	@Test
	public void testCreateCloudAclEntrySetWithAUserOwnerWillNotAddTheSameOwnerMoreThanOnce() throws NotOwnerException, LastOwnerException {
		TestUserImpl user1 = new TestUserImpl("user1");
		TestUserImpl user2 = new TestUserImpl("user2");
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(Sets.newHashSet(user1, user2));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user2));
		Assert.assertEquals(2, cloudAclEntrySet.getOwners().size());

		Assert.assertFalse(cloudAclEntrySet.addOwner(user1, user2));
		Assert.assertEquals(2, cloudAclEntrySet.getOwners().size());

		TestUserImpl user3 = new TestUserImpl("user3");
		Assert.assertTrue(cloudAclEntrySet.addOwner(user1, user3));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user2));
		Assert.assertTrue(cloudAclEntrySet.isOwner(user3));
		Assert.assertEquals(3, cloudAclEntrySet.getOwners().size());
	}

	@Test
	public void testCloudAclEntrySetWillNotAllowANonOwnerToMakeChangesToTheSet() {
		TestUserImpl user1 = new TestUserImpl("user1");
		TestUserImpl user2 = new TestUserImpl("user2");
		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(user1);
		Assert.assertTrue(cloudAclEntrySet.isOwner(user1));
		Assert.assertFalse(cloudAclEntrySet.isOwner(user2));
		
		try {
			cloudAclEntrySet.addAclEntry(user2, new CloudAclEntry<>(UserPrincipal.class));
			Assert.fail("Did not expect non-owner to be able to make changes to the set");
		} catch (NotOwnerException e) {
		}

		try {
			cloudAclEntrySet.addAllEntries(user2, Sets.newHashSet(new CloudAclEntry<>(UserPrincipal.class)));
			Assert.fail("Did not expect non-owner to be able to make changes to the set");
		} catch (NotOwnerException e) {
		}
		
		try {
			cloudAclEntrySet.clear(user2);
			Assert.fail("Did not expect non-owner to be able to make changes to the set");
		} catch (NotOwnerException e) {
		}

		try {
			cloudAclEntrySet.removeEntry(user2, new CloudAclEntry<>(UserPrincipal.class));
			Assert.fail("Did not expect non-owner to be able to make changes to the set");
		} catch (NotOwnerException e) {
		}

		try {
			cloudAclEntrySet.removeAll(user2, Sets.newHashSet(new CloudAclEntry<>(UserPrincipal.class)));
			Assert.fail("Did not expect non-owner to be able to make changes to the set");
		} catch (NotOwnerException e) {
		}


		try {
			cloudAclEntrySet.iterator(user2);
			Assert.fail("Did not expect non-owner to be able to make changes to the set");
		} catch (NotOwnerException e) {
		}
	}

	@Test
	public void testFindConflictingFindsAclEntriesWithAConflictingPrincipalType() throws NotOwnerException {
		CloudAclEntrySet aclEntrySet =
				new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, new DefaultCloudAclEntryConflictChecker());

		// These are the entries which we will add
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> blobAccessEntry =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PRIVATE))
					.build();
		CloudAclEntry<GroupPrincipal> groupEntry = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group1"))
				.setType(AclEntryType.ALLOW)
				.addPermission(AclEntryPermission.ADD_FILE)
				.build();
		CloudAclEntry<UserPrincipal> userEntry = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.ALLOW)
				.addPermission(AclEntryPermission.ADD_FILE)
				.build();
		
		// These are the set of conflicting permissions, simple denials of what we allow
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> conflictingBlobAccessEntry =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PUBLIC_READ))
					.build();
		CloudAclEntry<GroupPrincipal> conflictingGroupEntry = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group1"))
				.setType(AclEntryType.DENY)
				.addPermission(AclEntryPermission.ADD_FILE)
				.build();
		CloudAclEntry<UserPrincipal> conflictingUserEntry = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermission(AclEntryPermission.ADD_FILE)
				.build();

		// Should be no conflicts on the empty set
		Assert.assertTrue(aclEntrySet.findConflictingAcls(conflictingBlobAccessEntry).isEmpty());
		Assert.assertTrue(aclEntrySet.findConflictingAcls(conflictingGroupEntry).isEmpty());
		Assert.assertTrue(aclEntrySet.findConflictingAcls(conflictingUserEntry).isEmpty());

		// Add and Test PublicPrivateCloudPermissionsPrincipal
		Assert.assertTrue(aclEntrySet.addEntry(AnonymousUserPrincipal.INSTANCE, blobAccessEntry));
		Assert.assertEquals(1, aclEntrySet.size());
		assertConflictingAcls(aclEntrySet, conflictingBlobAccessEntry, blobAccessEntry);
		Assert.assertTrue(aclEntrySet.findConflictingAcls(conflictingGroupEntry).isEmpty());
		Assert.assertTrue(aclEntrySet.findConflictingAcls(conflictingUserEntry).isEmpty());

		// Add a new group
		Assert.assertTrue(aclEntrySet.addEntry(AnonymousUserPrincipal.INSTANCE, groupEntry));
		Assert.assertEquals(2, aclEntrySet.size());
		assertConflictingAcls(aclEntrySet, conflictingBlobAccessEntry, blobAccessEntry);
		assertConflictingAcls(aclEntrySet, conflictingGroupEntry, groupEntry);
		Assert.assertTrue(aclEntrySet.findConflictingAcls(conflictingUserEntry).isEmpty());

		// Add a new user
		Assert.assertTrue(aclEntrySet.addEntry(AnonymousUserPrincipal.INSTANCE, userEntry));
		Assert.assertEquals(3, aclEntrySet.size());
		assertConflictingAcls(aclEntrySet, conflictingBlobAccessEntry, blobAccessEntry);
		assertConflictingAcls(aclEntrySet, conflictingGroupEntry, groupEntry);
		assertConflictingAcls(aclEntrySet, conflictingUserEntry, userEntry);
	}
	
	void assertConflictingAcls(CloudAclEntrySet aclEntrySet, CloudAclEntry<?> newEntry, CloudAclEntry<?>... returnedConflicts) {
		Set<CloudAclEntry<?>> conflictingAcls = aclEntrySet.findConflictingAcls(newEntry);
		Assert.assertEquals(returnedConflicts.length, conflictingAcls.size());
		Arrays.stream(returnedConflicts).forEach(
				e -> Assert.assertTrue("Could not find expected conflicting ACL " + Arrays.asList(returnedConflicts) + " in the set " + conflictingAcls,
						conflictingAcls.contains(e)));
	}

	@Test
	public void testAddAclEntryWithTheForceOptionWillForceTheConflictingEntryIntoTheSet() throws NotOwnerException {
		CloudAclEntrySet aclEntrySet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, new DefaultCloudAclEntryConflictChecker());

		// Now add some entries
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> blobAccessEntry =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PRIVATE))
					.build();
		Assert.assertTrue(aclEntrySet.addEntry(AnonymousUserPrincipal.INSTANCE, blobAccessEntry));

		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> blobAccessEntry2 =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PUBLIC_READ))
					.build();

		// Without the force it should keep the old entry
		Assert.assertEquals(blobAccessEntry, aclEntrySet.addAclEntry(AnonymousUserPrincipal.INSTANCE, blobAccessEntry2, false).iterator().next());
		Assert.assertEquals(blobAccessEntry, aclEntrySet.iterator().next());
		
		// With the force it should replace the old entry
		Assert.assertEquals(blobAccessEntry, aclEntrySet.addAclEntry(AnonymousUserPrincipal.INSTANCE, blobAccessEntry2, true).iterator().next());
		Assert.assertEquals(blobAccessEntry2, aclEntrySet.iterator().next());
		Assert.assertEquals(1, aclEntrySet.size());
	}

	@Test
	public void testAddDoesNotAddConflictingEntries() throws NotOwnerException {
		UserPrincipal user1 = new TestUserImpl("user1");
		TestGroupImpl group2 = new TestGroupImpl("group1");
		CloudAclEntryConflictChecker checker = context.mock(CloudAclEntryConflictChecker.class);
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, checker);

		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry3 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group2)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.APPEND_DATA)
				.build();

		// This conflicts with entry 1
		CloudAclEntry<UserPrincipal> cloudAclEntry4 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();
		
		context.checking(new Expectations() {{
			allowing(checker).isConflictingAcl(cloudAclEntry1, cloudAclEntry2);
			will(returnValue(false));

			allowing(checker).isConflictingAcl(cloudAclEntry1, cloudAclEntry3);
			will(returnValue(false));

			allowing(checker).isConflictingAcl(cloudAclEntry2, cloudAclEntry3);
			will(returnValue(true));

			allowing(checker).isConflictingAcl(cloudAclEntry1, cloudAclEntry4);
			will(returnValue(true));

			allowing(checker).isConflictingAcl(cloudAclEntry2, cloudAclEntry4);
			will(returnValue(false));
		}});

		Assert.assertTrue(acls.addEntry(AnonymousUserPrincipal.INSTANCE, cloudAclEntry1));
		Assert.assertTrue(acls.addEntry(AnonymousUserPrincipal.INSTANCE, cloudAclEntry2));
		Assert.assertFalse(acls.addEntry(AnonymousUserPrincipal.INSTANCE, cloudAclEntry3));
		Assert.assertFalse(acls.addEntry(AnonymousUserPrincipal.INSTANCE, cloudAclEntry4));
	}

	@Test
	public void testAddAllDoesNotAddConflictingEntries() throws NotOwnerException {
		UserPrincipal user1 = new TestUserImpl("user1");
		TestGroupImpl group2 = new TestGroupImpl("group1");

		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry3 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.DELETE_CHILD)
				.build();

		// This conflicts with entry 1
		CloudAclEntry<UserPrincipal> cloudAclEntry4 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();
		
		// Confirm that the two rules should clash
		DefaultCloudAclEntryConflictChecker checker = new DefaultCloudAclEntryConflictChecker();
		Assert.assertTrue(checker.checkAllowDenyRulesHaveConflictingPermissions(cloudAclEntry1, cloudAclEntry4));

		// Run the method
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, checker);
		Assert.assertFalse(acls.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry1, cloudAclEntry2, cloudAclEntry3, cloudAclEntry4})));
		Assert.assertEquals("Not all ACL's were added: " + acls, 3, acls.size());
		Assert.assertTrue(acls.contains(cloudAclEntry1));
		Assert.assertTrue(acls.contains(cloudAclEntry2));
		Assert.assertTrue(acls.contains(cloudAclEntry3));
		Assert.assertFalse(acls.contains(cloudAclEntry4));
	}
	
	@Test
	public void testGetAclsWithPrincipalTypeReturnsAllAclsForAPrincipalType() throws NotOwnerException {
		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group1"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry3 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group2"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.DELETE_CHILD)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry4 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user2"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();
		
		DefaultCloudAclEntryConflictChecker checker = new DefaultCloudAclEntryConflictChecker();
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, checker);
		Assert.assertTrue(acls.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry1, cloudAclEntry2, cloudAclEntry3, cloudAclEntry4})));

		Set<CloudAclEntry<UserPrincipal>> userPrincipals = acls.findAclsWithPrincipalType(UserPrincipal.class);
		Assert.assertEquals(2, userPrincipals.size());
		Assert.assertTrue(userPrincipals.contains(cloudAclEntry1));
		Assert.assertTrue(userPrincipals.contains(cloudAclEntry4));

		Set<CloudAclEntry<GroupPrincipal>> groupPrincipals = acls.findAclsWithPrincipalType(GroupPrincipal.class);
		Assert.assertEquals(2, groupPrincipals.size());
		Assert.assertTrue(groupPrincipals.contains(cloudAclEntry2));
		Assert.assertTrue(groupPrincipals.contains(cloudAclEntry3));
	}
	
	@Test
	public void testCloneProducesACloneEqualsToTheOriginalSet() throws NotOwnerException {
		UserPrincipal user1 = new TestUserImpl("user1");
		TestGroupImpl group1 = new TestGroupImpl("group1");
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);

		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group1)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();
		
		Assert.assertTrue(acls.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry1, cloudAclEntry2})));
		
		CloudAclEntrySet clone = acls.clone();
		Assert.assertEquals(acls, clone);
	}

	@Test
	public void testOptimiseWillInvokeTheCheckerMergeAclsOperation() throws NotOwnerException {
		UserPrincipal user1 = new TestUserImpl("user1");
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);

		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE)
				.build();
		
		Assert.assertTrue(acls.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry1, cloudAclEntry2})));
		Assert.assertEquals(2, acls.size());

		// Optimise ACL's
		acls.optimise();
		Assert.assertEquals(1, acls.size());
		CloudAclEntry<UserPrincipal> expectedAclEntry = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.ADD_FILE)
				.build();
		Assert.assertEquals(expectedAclEntry, acls.stream().findFirst().get());
	}

	@Test
	public void testAddingWriteAclPermissionsForAUserAllowsANonOwnerOfTheAclToModifyAcls() throws NotOwnerException {
		UserPrincipal user1 = new TestUserImpl("user1");
		UserPrincipal user2 = new TestUserImpl("user2");
		CloudAclEntrySet acls = new CloudAclEntrySet(user1);

		// Ratify the read/write access
		assertReadWriteAccess(true, true, acls, user1);
		assertReadWriteAccess(false, false, acls, user2);

		// Set write access
		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.WRITE_ACL_PERMISSIONS)
				.build();
		Assert.assertTrue(acls.addAllEntries(user1, Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry1})));
		Assert.assertEquals(1, acls.size());
		
		// Test the any permissions search
		Set<CloudAclEntry<?>> aclsWithPerms =
				acls.findAclsOfTypeWithAnyPermissions(user2, AclEntryType.ALLOW, AclConstants.READ_WRITE_ACL_PERMISSIONS);
		Assert.assertEquals("Found incorrect ACL's: " + aclsWithPerms, 1, aclsWithPerms.size());

		// Test the all permissions search
		aclsWithPerms =
				acls.findAclsOfTypeWithAllPermissions(user2, AclEntryType.ALLOW, AclConstants.READ_WRITE_ACL_PERMISSIONS);
		Assert.assertTrue("Found incorrect ACL's: " + aclsWithPerms, aclsWithPerms.isEmpty());

		// Ratify the read/write access
		assertReadWriteAccess(false, true, acls, user2);

		// Add all read/write permissions
		cloudAclEntry1.getPermissions().addAll(AclConstants.READ_WRITE_ACL_PERMISSIONS);
		
		// Test the any permissions search
		aclsWithPerms =
				acls.findAclsOfTypeWithAnyPermissions(user2, AclEntryType.ALLOW, AclConstants.READ_WRITE_ACL_PERMISSIONS);
		Assert.assertEquals("Found incorrect ACL's: " + aclsWithPerms, 1, aclsWithPerms.size());
		Assert.assertEquals(cloudAclEntry1, aclsWithPerms.stream().findFirst().get());

		// Test the all permissions search
		aclsWithPerms =
				acls.findAclsOfTypeWithAllPermissions(user2, AclEntryType.ALLOW, AclConstants.READ_WRITE_ACL_PERMISSIONS);
		Assert.assertEquals("Found incorrect ACL's: " + aclsWithPerms, 1, aclsWithPerms.size());
		Assert.assertEquals(cloudAclEntry1, aclsWithPerms.stream().findFirst().get());

		// Ratify the read/write access
		assertReadWriteAccess(true, true, acls, user2);

		// Introduce another user
		UserPrincipal user3 = new TestUserImpl("user3");
		assertReadWriteAccess(false, false, acls, user3);
		
		// Set write access
		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user3)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.READ_WRITE_ACL_PERMISSIONS)
				.build();
		// Also test that user2 can write this ACL
		Assert.assertTrue(acls.addAllEntries(user2, Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry2})));
		Assert.assertEquals(2, acls.size());

		UserPrincipal user4 = new TestUserImpl("user4");
		assertReadWriteAccess(false, false, acls, user4);
		assertReadWriteAccess(true, true, acls, user3);
		assertReadWriteAccess(true, true, acls, user2);
		assertReadWriteAccess(true, true, acls, user1);
	}

	void assertReadWriteAccess(boolean readAccessExpected, boolean writeAccessExpected, CloudAclEntrySet acls, Principal principal) {
		// Ratify the read/write access
		try {
			acls.checkReadAccess(principal);
			if (!readAccessExpected) {
				Assert.fail("Did not expect read access");
			}
		} catch (NotOwnerException e) {
			if (readAccessExpected) {
				Assert.fail("Expected read access");
			}
		}

		try {
			acls.checkWriteAccess(principal);
			if (!writeAccessExpected) {
				Assert.fail("Did not expect write access");
			}
		} catch (NotOwnerException e) {
			if (writeAccessExpected) {
				Assert.fail("Expected write access");
			}
		}

		try {
			acls.checkReadWriteAccess(principal);
			if (!writeAccessExpected || !readAccessExpected) {
				Assert.fail("Did not expect read/write access");
			}
		} catch (NotOwnerException e) {
			if (writeAccessExpected && readAccessExpected) {
				Assert.fail("Expected read/write access");
			}
		}
	}
	
	@Test
	public void testGetAclEntriesUsesClonedEntriesAndDoesNotModifyTheUnderlyingAclEntry() throws NotOwnerException {
		UserPrincipal user1 = new TestUserImpl("user1");
		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user1)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_DIRECTORY_READ_PERMISSIONS)
				.build();
		CloudAclEntrySet acls = new CloudAclEntrySet(user1, cloudAclEntry1);

		Set<CloudAclEntry<?>> aclEntries = acls.getAclEntries();
		Assert.assertEquals(1, aclEntries.size());
		CloudAclEntry<?> cloudAclEntryClone = aclEntries.stream().findFirst().get();
		Assert.assertEquals(cloudAclEntry1, cloudAclEntryClone);
		Assert.assertFalse(cloudAclEntry1 == cloudAclEntryClone);
		cloudAclEntryClone.setPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS);
		Assert.assertEquals(AclConstants.ALL_FILE_WRITE_PERMISSIONS, cloudAclEntryClone.getPermissions());
		Assert.assertNotEquals(AclConstants.ALL_DIRECTORY_READ_PERMISSIONS, cloudAclEntryClone.getPermissions());
		Assert.assertEquals(AclConstants.ALL_DIRECTORY_READ_PERMISSIONS, cloudAclEntry1.getPermissions());
		Assert.assertNotEquals(AclConstants.ALL_FILE_WRITE_PERMISSIONS, cloudAclEntry1.getPermissions());
	}
	
}
