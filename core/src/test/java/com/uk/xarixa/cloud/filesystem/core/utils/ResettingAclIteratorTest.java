package com.uk.xarixa.cloud.filesystem.core.utils;

import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.acl.NotOwnerException;
import java.util.Arrays;
import java.util.HashSet;
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

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryConflictChecker;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.PublicPrivateCloudPermissionsPrincipal;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.TestGroupImpl;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.TestUserImpl;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;


@RunWith(BlockJUnit4ClassRunner.class)
public class ResettingAclIteratorTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	@Test
	public void testThatTheIteratorFiltersBasedOnAParticularClassType() throws NotOwnerException {
		CloudAclEntryConflictChecker conflictChecker = context.mock(CloudAclEntryConflictChecker.class);
		context.checking(new Expectations() {{
			allowing(conflictChecker).isConflictingAcl(with(any(CloudAclEntry.class)), with(any(CloudAclEntry.class)));
			will(returnValue(false));
		}});

		CloudAclEntry<?> pp1 = new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
				.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PRIVATE))
				.build();
		CloudAclEntry<?> user1 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user1"))
				.build();
		CloudAclEntry<?> user2 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user2"))
				.build();
		CloudAclEntry<?> user3 = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
				.setPrincipal(new TestUserImpl("user3"))
				.build();
		CloudAclEntry<?> group1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group1"))
				.build();
		CloudAclEntry<?> group2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group2"))
				.build();
		CloudAclEntry<?> group3 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group3"))
				.build();

		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, conflictChecker);
		cloudAclEntrySet.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {pp1, user1, group1, group2, user2, user3, group3}));
		Assert.assertEquals(7, cloudAclEntrySet.size());

		// Collate users from the iterator
		ResettingAclIterator aclIterator = new ResettingAclIterator(cloudAclEntrySet).reset(UserPrincipal.class);
		Set<CloudAclEntry<?>> allUsers = new HashSet<>();
		aclIterator.forEachRemaining(c -> allUsers.add(c));
		Assert.assertEquals(3, allUsers.size());
		Assert.assertTrue(allUsers.contains(user1));
		Assert.assertTrue(allUsers.contains(user2));
		Assert.assertTrue(allUsers.contains(user3));

		// Collate groups from the iterator
		aclIterator.reset(GroupPrincipal.class);
		Set<CloudAclEntry<?>> allGroups = new HashSet<>();
		aclIterator.forEachRemaining(c -> allGroups.add(c));
		Assert.assertEquals(3, allGroups.size());
		Assert.assertTrue(allGroups.contains(group1));
		Assert.assertTrue(allGroups.contains(group2));
		Assert.assertTrue(allGroups.contains(group3));

		// Collate public/private perms from the iterator
		aclIterator.reset(PublicPrivateCloudPermissionsPrincipal.class);
		Set<CloudAclEntry<?>> allPp = new HashSet<>();
		aclIterator.forEachRemaining(c -> allPp.add(c));
		Assert.assertEquals(1, allPp.size());
		Assert.assertTrue(allPp.contains(pp1));
	}

	@Test
	public void testThatTheIteratorFiltersFindsNoResultsForAParticularClassType() throws NotOwnerException {
		CloudAclEntryConflictChecker conflictChecker = context.mock(CloudAclEntryConflictChecker.class);
		context.checking(new Expectations() {{
			allowing(conflictChecker).isConflictingAcl(with(any(CloudAclEntry.class)), with(any(CloudAclEntry.class)));
			will(returnValue(false));
		}});

		CloudAclEntry<?> pp1 = new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
				.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PRIVATE))
				.build();
		CloudAclEntry<?> group1 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group1"))
				.build();
		CloudAclEntry<?> group2 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group2"))
				.build();
		CloudAclEntry<?> group3 = new CloudAclEntryBuilder<GroupPrincipal>(GroupPrincipal.class)
				.setPrincipal(new TestGroupImpl("group3"))
				.build();

		CloudAclEntrySet cloudAclEntrySet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, conflictChecker);
		cloudAclEntrySet.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {pp1, group1, group2, group3}));
		Assert.assertEquals(4, cloudAclEntrySet.size());

		// Collate users from the iterator
		ResettingAclIterator aclIterator = new ResettingAclIterator(cloudAclEntrySet).reset(UserPrincipal.class);
		Assert.assertFalse(aclIterator.hasNext());
		Set<CloudAclEntry<?>> allUsers = new HashSet<>();
		aclIterator.forEachRemaining(c -> allUsers.add(c));
		Assert.assertTrue(allUsers.isEmpty());

		// Collate groups from the iterator
		aclIterator.reset(GroupPrincipal.class);
		Set<CloudAclEntry<?>> allGroups = new HashSet<>();
		aclIterator.forEachRemaining(c -> allGroups.add(c));
		Assert.assertEquals(3, allGroups.size());
		Assert.assertTrue(allGroups.contains(group1));
		Assert.assertTrue(allGroups.contains(group2));
		Assert.assertTrue(allGroups.contains(group3));

		// Collate public/private perms from the iterator
		aclIterator.reset(PublicPrivateCloudPermissionsPrincipal.class);
		Set<CloudAclEntry<?>> allPp = new HashSet<>();
		aclIterator.forEachRemaining(c -> allPp.add(c));
		Assert.assertEquals(1, allPp.size());
		Assert.assertTrue(allPp.contains(pp1));
	}

}
