package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.NotOwnerException;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;

@RunWith(BlockJUnit4ClassRunner.class)
public class DefaultCloudAclEntryConflictCheckerTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	private DefaultCloudAclEntryConflictChecker checker;


	@Before
	public void setUp() {
		checker = new DefaultCloudAclEntryConflictChecker();
	}

	@Test
	public void testIsConflictingAclWillDetermineThatAPublicAndPrivateCloudAclConflicts() {
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> privateAccessEntry =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PRIVATE))
					.setType(AclEntryType.ALLOW)
					.build();
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> publicAccessEntry =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PUBLIC_READ))
					.setType(AclEntryType.ALLOW)
					.build();

		Assert.assertFalse(checker.isConflictingAcl(privateAccessEntry, privateAccessEntry));
		Assert.assertTrue(checker.isConflictingAcl(privateAccessEntry, publicAccessEntry));
		Assert.assertTrue(checker.isConflictingAcl(publicAccessEntry, privateAccessEntry));
		Assert.assertFalse(checker.isConflictingAcl(publicAccessEntry, publicAccessEntry));
	}

	@Test
	public void testIsConflictingAclWillDetermineThatAnAllowAndDenyForTheSamePermissionsForAUserConflicts() {
		UserPrincipal user1 = new TestUserImpl("user1");
		UserPrincipal user2 = new TestUserImpl("user1");

		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.setPrincipal(user1)
			.setType(AclEntryType.DENY)
			.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
			.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.DELETE, AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		Assert.assertTrue(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIsConflictingAclWillDetermineThatAnAllowAndDenyForDifferentUsersDoesNotConflict() {
		UserPrincipal user1 = new TestUserImpl("user1");
		UserPrincipal user2 = new TestUserImpl("user2");

		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.setPrincipal(user1)
			.setType(AclEntryType.DENY)
			.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
			.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(user2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.DELETE, AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		Assert.assertFalse(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIsConflictingAclWillDetermineThatAnAllowAndDenyForTheSamePermissionsForAGroupConflicts() {
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group1");

		CloudAclEntry<GroupPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.setPrincipal(group1)
			.setType(AclEntryType.DENY)
			.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
			.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		Assert.assertTrue(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIsConflictingAclWillDetermineThatAnAllowAndDenyForDifferentGroupsDoesNotConflict() {
		GroupPrincipal group1 = new TestGroupImpl("group1");
		GroupPrincipal group2 = new TestGroupImpl("group2");

		CloudAclEntry<GroupPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
			.setPrincipal(group1)
			.setType(AclEntryType.DENY)
			.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
			.build();

		CloudAclEntry<GroupPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(group2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_SUBDIRECTORY)
				.build();

		Assert.assertFalse(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIsConflictingAclWillReturnFalseForAUserNotInAGroup() {
		UserPrincipal user1 = new TestUserImpl("user1");
		GroupPrincipal group2 = new TestGroupImpl("group1");

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

		Assert.assertFalse(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIsConflictingAclWillReturnFalseByDefaultForAUserInAGroupWithConflictingPermissions() {
		UserPrincipal user1 = new TestUserImpl("user1");
		TestGroupImpl group2 = new TestGroupImpl("group1");
		group2.addMember(user1);
		Assert.assertTrue(group2.isMember(user1));

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

		Assert.assertFalse(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIsConflictingAclWillReturnTrueIfTheGroupMembershipCheckIsEnabledForAUserInAGroupWithConflictingPermissions() {
		checker = new DefaultCloudAclEntryConflictChecker(true);
		UserPrincipal user1 = new TestUserImpl("user1");
		TestGroupImpl group2 = new TestGroupImpl("group1");
		group2.addMember(user1);
		Assert.assertTrue(group2.isMember(user1));

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

		Assert.assertTrue(checker.isConflictingAcl(cloudAclEntry1, cloudAclEntry2));
	}

	@Test
	public void testIterateAcrossAllAclsOnlyIteratesAcrossTwoAclsOnce() throws NotOwnerException {
		CloudAclEntry<?> user1 = createCloudAclEntryMock("user1", UserPrincipal.class);
		CloudAclEntry<?> group1 = createCloudAclEntryMock("group1", GroupPrincipal.class);
		CloudAclEntry<?> group2 = createCloudAclEntryMock("group2", GroupPrincipal.class);
		CloudAclEntry<?> user2 = createCloudAclEntryMock("user2", UserPrincipal.class);
		CloudAclEntry<?> user3 = createCloudAclEntryMock("user3", UserPrincipal.class);
		CloudAclEntry<?> group3 = createCloudAclEntryMock("group3", GroupPrincipal.class);

		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, checker);
		cloudAclEntrySet.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {user1, group1, group2, user2, user3, group3}));
		Assert.assertEquals(6, cloudAclEntrySet.size());

		// Get all of the entries into a map. If any cannot be put into the map it means that the two entries
		// have been seen previously.
		HashSetValuedHashMap<CloudAclEntry<?>, CloudAclEntry<?>> map = new HashSetValuedHashMap<>();
		checker.iterateAcrossAllAcls(true, cloudAclEntrySet,
				(entry1 ,entry2) -> {Assert.assertTrue("Two entries in the map have been seen before: " + entry1 + ", " + entry2 + ", map=" + map.toString(),
											map.put(entry1, entry2));}
		);

		// Now check the map entries...
		Assert.assertEquals(6, map.size());
		//System.out.println(map.toString());
		HashSetValuedHashMap<Class<?>,CloudAclEntry<?>> cloudAclEntriesByPrincipalClass = new HashSetValuedHashMap<>();
		for (CloudAclEntry<?> key : map.keySet()) {
			cloudAclEntriesByPrincipalClass.put(key.getPrincipalClass(), key);
			Set<CloudAclEntry<?>> values = map.get(key);
			
			for (CloudAclEntry<?> value : values) {
				cloudAclEntriesByPrincipalClass.put(value.getPrincipalClass(), value);

				// Now check the map entry for this value, the key should not be in this set
				Set<CloudAclEntry<?>> valueEntries = map.get(value);
				
				// The values may not exist
				if (valueEntries != null) {
					Assert.assertFalse("Two entries in the map have been seen before: " + key + ", " + value + ", map=" + map.toString(),
						valueEntries.contains(key));
				}
			}
		}

		// Make sure that we have vistited all of the ACL's which we defined in the list
		Assert.assertEquals(3, cloudAclEntriesByPrincipalClass.get(UserPrincipal.class).size());
		Assert.assertEquals(3, cloudAclEntriesByPrincipalClass.get(GroupPrincipal.class).size());
	}
	
	@Test
	public void testMergeAclForTwoAllowRulesWillMergeThePermissionsAndFlagsOfTwoAclsForTheSameUser() {
		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.addFlag(AclEntryFlag.DIRECTORY_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.DELETE)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntry<?> mergedAcl = checker.mergeAcl(new ConflictingCloudAclEntry(cloudAclEntry1, cloudAclEntry2));
		Assert.assertEquals("user1", ((TestUserImpl)mergedAcl.getPrincipal()).getName());
		Assert.assertEquals(AclEntryType.ALLOW, mergedAcl.getType());
		Assert.assertEquals(EnumSet.of(AclEntryPermission.DELETE, AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY),
				mergedAcl.getPermissions());
		Assert.assertEquals(EnumSet.of(AclEntryFlag.DIRECTORY_INHERIT, AclEntryFlag.FILE_INHERIT), mergedAcl.getFlags());
	}
	
	@Test
	public void testMergeAclForTwoDenyRulesWillMergeThePermissionsAndFlagsOfTwoAclsForTheSameUser() {
		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.addFlag(AclEntryFlag.DIRECTORY_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.DELETE)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntry<?> mergedAcl = checker.mergeAcl(new ConflictingCloudAclEntry(cloudAclEntry1, cloudAclEntry2));
		Assert.assertEquals("user1", ((TestUserImpl)mergedAcl.getPrincipal()).getName());
		Assert.assertEquals(AclEntryType.DENY, mergedAcl.getType());
		Assert.assertEquals(EnumSet.of(AclEntryPermission.DELETE, AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY),
				mergedAcl.getPermissions());
		Assert.assertEquals(EnumSet.of(AclEntryFlag.DIRECTORY_INHERIT, AclEntryFlag.FILE_INHERIT), mergedAcl.getFlags());
	}
	
	@Test
	public void testMergeAclsWillMergeAllMergeableAclsInTheSet() {
		CloudAclEntry<UserPrincipal> cloudAclEntry1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)
				.addFlag(AclEntryFlag.DIRECTORY_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.DELETE)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry3 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user2"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry4 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.setType(AclEntryType.DENY)
				.addPermissions(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry5 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user3"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntry<UserPrincipal> cloudAclEntry6 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user2"))
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclEntryPermission.DELETE)
				.addFlag(AclEntryFlag.FILE_INHERIT)
				.build();

		CloudAclEntrySet entrySet = checker.mergeAcls(AnonymousUserPrincipal.INSTANCE, cloudAclEntry1, cloudAclEntry2, cloudAclEntry3, cloudAclEntry4, cloudAclEntry5, cloudAclEntry6);
		Assert.assertTrue(entrySet.isOwner(AnonymousUserPrincipal.INSTANCE));
		Assert.assertEquals(3, entrySet.size());
		Iterator<CloudAclEntry<?>> iterator = entrySet.iterator();
		while (iterator.hasNext()) {
			CloudAclEntry<?> entry = iterator.next();
			String userName = ((TestUserImpl)entry.getPrincipal()).getName();

			switch (userName) {
				case "user1": 	Assert.assertEquals(AclEntryType.DENY, entry.getType());
								Assert.assertEquals(EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY,
										AclEntryPermission.DELETE, AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA),
										entry.getPermissions());
								break;
				case "user2": 	Assert.assertEquals(AclEntryType.ALLOW, entry.getType());
								Assert.assertEquals(EnumSet.of(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA,
										AclEntryPermission.DELETE),
										entry.getPermissions());
								break;
				case "user3": 	Assert.assertEquals(AclEntryType.ALLOW, entry.getType());
								Assert.assertEquals(EnumSet.of(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA),
										entry.getPermissions());
								break;
				default: Assert.fail("Unexpected user entry " + entry);
			}
		}
	}

	CloudAclEntry<?> createCloudAclEntryMock(String name, Class<? extends Principal> principalClass) {
		CloudAclEntry<?> mock = context.mock(CloudAclEntry.class, name);
		
		Principal principal;
		if (principalClass.equals(UserPrincipal.class)) {
			principal = new TestUserImpl(name);
		} else if (principalClass.equals(GroupPrincipal.class)) {
			principal = new TestGroupImpl(name);
		} else {
			throw new IllegalArgumentException("Cannot create principal class of type " + principalClass);
		}
		
		context.checking(new Expectations() {{
			allowing(mock).getPrincipalClass();
			will(returnValue(principalClass));
			
			allowing(mock).getPrincipal();
			will(returnValue(principal));
			
			allowing(mock).compareTo(with(any(CloudAclEntry.class)));
			will(returnValue(-1));
			
			allowing(mock).clone();
			will(returnValue(mock));
		}});
		
		return mock;
	}

}
