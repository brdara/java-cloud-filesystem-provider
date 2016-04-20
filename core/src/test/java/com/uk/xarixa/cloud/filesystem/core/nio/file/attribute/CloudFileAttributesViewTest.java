package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.security.acl.NotOwnerException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudFileAttributesViewTest {
	private static final String TEST_PATH = "test-path";
	private static final String TEST_CONTAINER = "test-container";
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};
	private BlobStoreContext blobStoreContext;
	private CloudPath cloudPath;
	private CloudFileAttributesView view;
	private BlobStore blobStore;
	private FileSystemProvider provider;
	private CloudFileSystem fs;
	private CloudHostConfiguration config;

	@Before
	public void setUp() {
		blobStore = context.mock(BlobStore.class);
		blobStoreContext = context.mock(BlobStoreContext.class);
		cloudPath = context.mock(CloudPath.class);
		view = new CloudFileAttributesView(blobStoreContext, cloudPath);
		provider = context.mock(FileSystemProvider.class);
		fs = context.mock(CloudFileSystem.class);
		config = context.mock(CloudHostConfiguration.class);

		context.checking(new Expectations() {{
			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));
			
        	allowing(fs).provider();
        	will(returnValue(provider));
        	
        	allowing(fs).getCloudHostConfiguration();
        	will(returnValue(config));
        	
        	allowing(cloudPath).getFileSystem();
        	will(returnValue(fs));
        	
        	allowing(cloudPath).getContainerName();
        	will(returnValue(TEST_CONTAINER));

        	allowing(cloudPath).getPathName();
        	will(returnValue(TEST_PATH));
		}});
	}


	@Test
	public void testCheckAccessForADirectoryWillReturnIfNoCloudHostSecurityManagerIsConfiguredByDefault() throws IOException {
		context.checking(new Expectations() {{
			allowing(config).getUserGroupLookupService();
			will(returnValue(null));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(null));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));
		}});

		view.checkAccess(EnumSet.of(AclEntryPermission.ADD_FILE));
	}

	@Test
	public void testCheckAccessForADirectoryWithAnAnonymousUserWillThrowAAccessDeniedExceptionIfTheSecurityManagerDoesNotAllowAccess() throws IOException {
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		EnumSet<AclEntryPermission> perms = EnumSet.of(AclEntryPermission.ADD_FILE);

		context.checking(new Expectations() {{
			allowing(config).getUserGroupLookupService();
			will(returnValue(null));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));
			
			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			exactly(1).of(securityManager).checkAccessAllowed(with(any(CloudAclEntrySet.class)),
					with(equal(AnonymousUserPrincipal.INSTANCE)), with(equal(perms)));
			will(returnValue(false));
		}});

		try {
			view.checkAccess(perms);
			Assert.fail("Did not expect to have access");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	@Test
	public void testCheckAccessForADirectoryWithAnAnonymousUserWillAllowAccessIfTheSecurityManagerAllowsIt() throws IOException {
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		EnumSet<AclEntryPermission> perms = EnumSet.of(AclEntryPermission.ADD_FILE);

		context.checking(new Expectations() {{
			allowing(config).getUserGroupLookupService();
			will(returnValue(null));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			exactly(1).of(securityManager).checkAccessAllowed(with(any(CloudAclEntrySet.class)),
					with(equal(AnonymousUserPrincipal.INSTANCE)), with(equal(perms)));
			will(returnValue(true));
		}});

		view.checkAccess(perms);
	}

	@Test
	public void testCheckAccessForADirectoryWithACurrentUserWillThrowAAccessDeniedExceptionIfTheSecurityManagerDoesNotAllowAccess() throws IOException {
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> perms = EnumSet.of(AclEntryPermission.ADD_FILE);

		context.checking(new Expectations() {{
			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			exactly(1).of(securityManager).checkAccessAllowed(with(any(CloudAclEntrySet.class)),
					with(equal(currentUser)), with(equal(perms)));
			will(returnValue(false));
		}});

		try {
			view.checkAccess(perms);
			Assert.fail("Did not expect to have access");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	@Test
	public void testCheckAccessWithACurrentUserAllowAccessWhenTheSecurityManagerAllowsIt() throws IOException {
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> perms = EnumSet.of(AclEntryPermission.ADD_FILE);

		context.checking(new Expectations() {{
			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			exactly(1).of(securityManager).checkAccessAllowed(with(any(CloudAclEntrySet.class)),
					with(equal(currentUser)), with(equal(perms)));
			will(returnValue(true));
		}});

		view.checkAccess(perms);
	}

	@Test
	public void testSetAclFileAttributesWillOnlySetThePublicPrivateCloudPermissionsPrincipalWhenTheSecurityManagerAllowsIt() throws NotOwnerException, IOException {
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		UserPrincipal user1 = new TestUserImpl("user1");
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		EnumSet<AclEntryPermission> perms = EnumSet.of(AclEntryPermission.WRITE_ACL);

		// These two entries will be merged
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

		// This is the only entry which can be applied
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> cloudAclEntry3 = new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
				.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PUBLIC_READ))
				.setType(AclEntryType.ALLOW)
				.addPermissions()
				.build();

		// Add the ACL's to the set
		Assert.assertTrue(acls.addAllEntries(AnonymousUserPrincipal.INSTANCE,
				Arrays.asList(new CloudAclEntry<?>[] {cloudAclEntry1, cloudAclEntry2, cloudAclEntry3})));
		Assert.assertEquals(3, acls.size());
		
		context.checking(new Expectations() {{
			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			exactly(1).of(securityManager).checkAccessAllowed(with(any(CloudAclEntrySet.class)),
					with(equal(currentUser)), with(equal(perms)));
			will(returnValue(true));

			exactly(1).of(blobStore).setBlobAccess(TEST_CONTAINER, TEST_PATH, BlobAccess.PUBLIC_READ);
		}});

		// Now invoke the view method to set the ACL's
		Set<CloudAclEntry<?>> unsetAttributes = view.setAclFileAttributes(acls);
		Assert.assertEquals(1, unsetAttributes.size());
		CloudAclEntry<?> entry = unsetAttributes.stream().findFirst().get();
		Assert.assertTrue(
				entry.getPermissions().containsAll(
						Sets.newHashSet(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY)));
	}

	@Test
	public void testSetTimesThrowsAnException() throws IOException {
		try {
			view.setTimes(FileTime.fromMillis(0L), FileTime.fromMillis(0L), FileTime.fromMillis(0L));
			Assert.fail();
		} catch (UnsupportedOperationException e) {
			// OK
		}
	}

}
