package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.NotImplementedException;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.options.GetOptions;
import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.powermock.reflect.internal.WhiteboxImpl;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.PublicPrivateCloudPermissionsPrincipal;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.TestUserImpl;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.NotOwnerException;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

@RunWith(BlockJUnit4ClassRunner.class)
public class DefaultCloudFileSystemImplementationTest {
	private static final String TEST_PATH = "test-path";
	private static final String TEST_CONTAINER = "test-container";

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	private DefaultCloudFileSystemImplementation impl;
	private FileSystemProvider provider;
	private CloudFileSystem fs;
	private CloudHostConfiguration config;

	@Before
	public void setUp() {
		impl = new DefaultCloudFileSystemImplementation();
		provider = context.mock(FileSystemProvider.class);
		fs = context.mock(CloudFileSystem.class);
		config = context.mock(CloudHostConfiguration.class);
		
        context.checking(new Expectations() {{
        	allowing(fs).provider();
        	will(returnValue(provider));
        	
        	allowing(fs).getCloudHostConfiguration();
        	will(returnValue(config));
        }});
	}

	@Test
	public void testGetFileAttributeViewReturnsNullForAnUnknownAttributeView() {
		CloudPath path = context.mock(CloudPath.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		Assert.assertNull(impl.getFileAttributeView(blobStoreContext, PosixFileAttributeView.class, path));
	}

	@Test
	public void testGetFileAttributeViewCreatesACloudFileAttributesViewInstance() {
		CloudPath path = context.mock(CloudPath.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		Assert.assertNotNull(impl.getFileAttributeView(blobStoreContext, CloudFileAttributesView.class, path));
	}
	
	@Test
	public void testGetFileAttributeViewCreatesABasicFileAttributesViewInstance() {
		CloudPath path = context.mock(CloudPath.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BasicFileAttributeView view = impl.getFileAttributeView(blobStoreContext, BasicFileAttributeView.class, path);
		Assert.assertNotNull(view);
		Assert.assertTrue(view instanceof CloudFileAttributesView);
	}

	@Test
	public void testSetAttributeCreatesANewCloudFileAttributesViewAndCallsItsSetter() throws NotOwnerException, IOException {
		CloudPath path = context.mock(CloudPath.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudFileSystemImplementation cloudFileSystemImplementation = context.mock(CloudFileSystemImplementation.class);
		UserPrincipal user1 = new TestUserImpl("user1");
		
		// Create ACL's
		CloudAclEntrySet acls = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class);

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
			allowing(provider).readAttributes(path, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));

			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));

			allowing(path).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));
			
			allowing(config).getCloudHostSecurityManager();
			will(returnValue(null));

			allowing(cloudFileSystemImplementation).checkAccess(blobStoreContext, path, CloudFileAttributesView.WRITE_ACL_PERMS);

			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			// Set the public read BLOB access
			exactly(1).of(blobStore).setBlobAccess(TEST_CONTAINER, TEST_PATH, BlobAccess.PUBLIC_READ);
		}});

		impl.setAttribute(blobStoreContext, path, DefaultCloudFileSystemImplementation.ACL_SET_ATTRIBUTE, acls);
	}

	@Test
	public void testNewByteChannelCannotCreateAFileIfTheParentPathDoesNotAllowFileAdd() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		CloudPath parentPath = context.mock(CloudPath.class, "parentPath");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> perms = EnumSet.of(AclEntryPermission.ADD_FILE);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class);
		CloudAclEntrySet aclEntrySet = createAclEntrySetMock();

		context.checking(new Expectations() {{
			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));

			allowing(path).getParent();
			will(returnValue(parentPath));
			
			allowing(provider).readAttributes(parentPath, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));
			
			allowing(aclFileAttributes).getAclSet();
			will(returnValue(aclEntrySet));

			allowing(parentPath).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));

			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));
			
			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));

			allowing(parentPath).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(parentPath).getPathName();
			will(returnValue(TEST_PATH));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			exactly(1).of(securityManager).checkAccessAllowed(aclEntrySet, currentUser, perms);
			will(returnValue(false));
		}});

		try {
			impl.newByteChannel(blobStoreContext, path, Sets.newHashSet(StandardOpenOption.CREATE_NEW));
			Assert.fail("Did not expect to be able to create a new byte channel");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	@Test
	public void testNewByteChannelCannotCreateAFileIfTheParentPathDoesNotAllowFileWrite() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		CloudPath parentPath = context.mock(CloudPath.class, "parentPath");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> parentPathPerms = EnumSet.of(AclEntryPermission.ADD_FILE);
		EnumSet<AclEntryPermission> pathPerms = EnumSet.of(AclEntryPermission.APPEND_DATA);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class, "aclFileAttributes");
		CloudAclEntrySet aclEntrySet = createAclEntrySetMock("aclEntrySet");
		CloudAclFileAttributes parentAclFileAttributes = context.mock(CloudAclFileAttributes.class, "parentAclFileAttributes");
		CloudAclEntrySet parentAclEntrySet = createAclEntrySetMock("parentAclEntrySet");

		context.checking(new Expectations() {{
			allowing(path).getParent();
			will(returnValue(parentPath));
			
			allowing(provider).readAttributes(path, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));
			
			allowing(aclFileAttributes).getAclSet();
			will(returnValue(aclEntrySet));

			allowing(provider).readAttributes(parentPath, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(parentAclFileAttributes));
			
			allowing(parentAclFileAttributes).getAclSet();
			will(returnValue(parentAclEntrySet));

			allowing(path).getFileSystem();
			will(returnValue(fs));
			
			allowing(parentPath).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));

			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			allowing(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));
			
			allowing(parentPath).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(parentPath).getPathName();
			will(returnValue(TEST_PATH));
			
			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));
			
			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));
			
			allowing(path).exists();
			will(returnValue(false));

			// Parent path access check
			exactly(1).of(securityManager).checkAccessAllowed(parentAclEntrySet, currentUser, parentPathPerms);
			will(returnValue(false));
		}});

		try {
			impl.newByteChannel(blobStoreContext, path, Sets.newHashSet(StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND));
			Assert.fail("Did not expect to be able to create a new byte channel");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	@Test
	public void testNewByteChannelCanOpenAFileForReadingIfTheUserHasAccessRights() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> pathPerms = EnumSet.of(AclEntryPermission.READ_DATA);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class, "aclFileAttributes");
		CloudAclEntrySet aclEntrySet = createAclEntrySetMock("aclEntrySet");

		context.checking(new Expectations() {{
			allowing(provider).readAttributes(path, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));
			
			allowing(aclFileAttributes).getAclSet();
			will(returnValue(aclEntrySet));
			
			allowing(path).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));

			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			allowing(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));

			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));
			
			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));
			
			allowing(path).exists();
			will(returnValue(true));

			// Path access check
			exactly(1).of(securityManager).checkAccessAllowed(aclEntrySet, currentUser, pathPerms);
			will(returnValue(true));
			
			// From within CloudFileChannel init this is invoked, this is how we can tell when the method has succeeded
			exactly(1).of(blobStore).getBlob(with(equal(TEST_CONTAINER)), with(equal(TEST_PATH)), with(any(GetOptions.class)));
			will(throwException(new NotImplementedException("It's OK to fail here")));
		}});

		try {
			impl.newByteChannel(blobStoreContext, path, Sets.newHashSet(StandardOpenOption.READ));
			Assert.fail("Did not expect success");
		} catch (NotImplementedException e) {
			// OK
		}
	}

	@Test
	public void testNewByteChannelCannotOpenAFileForReadingIfTheUserDoesntHaveAccessRights() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> pathPerms = EnumSet.of(AclEntryPermission.READ_DATA);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class);
		CloudAclEntrySet aclEntrySet = createAclEntrySetMock();

		context.checking(new Expectations() {{
			allowing(provider).readAttributes(path, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));
			
			allowing(aclFileAttributes).getAclSet();
			will(returnValue(aclEntrySet));

			allowing(path).exists();
			will(returnValue(true));

			allowing(path).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));

			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			allowing(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));
			
			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));
			
			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));
			
			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			// Path access check
			exactly(1).of(securityManager).checkAccessAllowed(aclEntrySet, currentUser, pathPerms);
			will(returnValue(false));
		}});

		try {
			impl.newByteChannel(blobStoreContext, path, Sets.newHashSet(StandardOpenOption.READ));
			Assert.fail("Did not expect to be able to create a new byte channel");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	protected CloudAclEntrySet createAclEntrySetMock() {
		return createAclEntrySetMock("aclEntrySet");
	}

	protected CloudAclEntrySet createAclEntrySetMock(String name) {
		CloudAclEntrySet aclEntrySet = context.mock(CloudAclEntrySet.class, name);
		// The next part we have to do because of the aclEntrySet.equals method which uses this,
		// so we have to set this value in the mock or we get an NPE
		WhiteboxImpl.setInternalState(aclEntrySet, "ownersLock", new ReentrantReadWriteLock());
		return aclEntrySet;
	}

	@Test
	public void testNewDirectoryStreamFailsIfTheUserDoesntHaveAccessRights() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> pathPerms = EnumSet.of(AclEntryPermission.LIST_DIRECTORY);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class);
		CloudAclEntrySet aclEntrySet = createAclEntrySetMock();

		context.checking(new Expectations() {{
			allowing(provider).readAttributes(path, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));
			
			allowing(aclFileAttributes).getAclSet();
			will(returnValue(aclEntrySet));

			allowing(path).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));

			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			allowing(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));
			
			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));
			
			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));

			// Path access check
			exactly(1).of(securityManager).checkAccessAllowed(aclEntrySet, currentUser, pathPerms);
			will(returnValue(false));
		}});

		try {
			impl.newDirectoryStream(blobStoreContext, path, null);
			Assert.fail("Did not expect to be able to read the directory without perms");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	@Test
	public void testCreateDirectoryFailsIfTheUserDoesntHaveAccessRightsToCreateADirectoryInTheParentPath() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		CloudPath parentPath = context.mock(CloudPath.class, "parentPath");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		BlobStore blobStore = context.mock(BlobStore.class);
		CloudHostSecurityManager securityManager = context.mock(CloudHostSecurityManager.class);
		UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		UserPrincipal currentUser = context.mock(UserPrincipal.class);
		EnumSet<AclEntryPermission> pathPerms = EnumSet.of(AclEntryPermission.ADD_SUBDIRECTORY);
		CloudAclFileAttributes aclFileAttributes = context.mock(CloudAclFileAttributes.class);
		CloudAclEntrySet aclEntrySet = createAclEntrySetMock();

		context.checking(new Expectations() {{
			allowing(blobStoreContext).getBlobStore();
			will(returnValue(blobStore));

			allowing(provider).readAttributes(parentPath, CloudAclFileAttributes.class, new LinkOption[0]);
			will(returnValue(aclFileAttributes));
			
			allowing(aclFileAttributes).getAclSet();
			will(returnValue(aclEntrySet));

			allowing(path).exists();
			will(returnValue(false));

			allowing(parentPath).exists();
			will(returnValue(true));

			allowing(path).getParent();
			will(returnValue(parentPath));

			allowing(parentPath).getFileSystem();
			will(returnValue(fs));
			
			allowing(fs).getCloudHostConfiguration();
			will(returnValue(config));

			allowing(config).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			allowing(lookupService).getCurrentUser();
			will(returnValue(currentUser));

			allowing(config).getCloudHostSecurityManager();
			will(returnValue(securityManager));

			allowing(path).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(path).getPathName();
			will(returnValue(TEST_PATH));

			allowing(parentPath).getContainerName();
			will(returnValue(TEST_CONTAINER));

			allowing(parentPath).getPathName();
			will(returnValue(TEST_PATH));

			allowing(blobStore).directoryExists(TEST_CONTAINER, TEST_PATH);
			will(returnValue(true));
			
			// Path access check
			exactly(1).of(securityManager).checkAccessAllowed(aclEntrySet, currentUser, pathPerms);
			will(returnValue(false));
		}});

		try {
			impl.createDirectory(blobStoreContext, path);
			Assert.fail("Did not expect to be able to create the directory without perms");
		} catch (AccessDeniedException e) {
			// OK
		}
	}

	@Test
	public void testCreateDirectoryFailsIfThePathExists() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);

		context.checking(new Expectations() {{
			allowing(path).exists();
			will(returnValue(true));
			
			allowing(path).toAbsolutePath();
			will(returnValue(path));
		}});

		try {
			impl.createDirectory(blobStoreContext, path, new FileAttribute[0]);
			Assert.fail("Did not expect to be able to create the directory when path exists");
		} catch (FileAlreadyExistsException e) {
			// OK
		}
	}

	@Test
	public void testCreateDirectoryFailsIfTheParentPathDoesNotExist() throws IOException {
		CloudPath path = context.mock(CloudPath.class, "path");
		CloudPath parentPath = context.mock(CloudPath.class, "parentPath");
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);
		
		context.checking(new Expectations() {{
			allowing(path).exists();
			will(returnValue(false));
			
			allowing(path).getPathName();
			will(returnValue("path-with-a-container"));
			
			allowing(path).toAbsolutePath();
			will(returnValue(path));

			exactly(1).of(path).getParent();
			will(returnValue(parentPath));

			allowing(parentPath).exists();
			will(returnValue(false));
		}});

		try {
			impl.createDirectory(blobStoreContext, path, new FileAttribute[0]);
			Assert.fail("Did not expect to be able to create the directory without parent path");
		} catch (IOException e) {
			// OK
		}
	}

}
