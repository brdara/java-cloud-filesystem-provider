package com.uk.xarixa.cloud.filesystem.core.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;

import org.jclouds.blobstore.domain.StorageType;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.AclConstants;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.TestUserImpl;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudFileTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};
	
	@Test
	public void testIsDirectoryReturnsTrueForAContainer() throws IOException {
		final String container = "blah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container));
		mockStorageTypeAttributes(fs, StorageType.CONTAINER, cf.toPath());
		Assert.assertTrue(cf.isDirectory());
	}

	@Test
	public void testIsDirectoryReturnsTrueForADirectoryStorageType() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, StorageType.FOLDER, cf.toPath());
		Assert.assertTrue(cf.isDirectory());
	}
	
	@Test
	public void testIsDirectoryReturnsFalseWhenTheBlobDoesntExist() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, null, cf.toPath());
		Assert.assertFalse(cf.isDirectory());
	}

	protected CloudBasicFileAttributes mockStorageTypeAttributes(CloudFileSystem fs, final StorageType storageType, final Path path) throws IOException {
		final FileSystemProvider provider = context.mock(FileSystemProvider.class);
		final CloudBasicFileAttributes basicAttributes = storageType == null ? null : context.mock(CloudBasicFileAttributes.class);
		
		context.checking(new Expectations() {{
			allowing(fs).provider();
			will(returnValue(provider));

			if (basicAttributes == null) {
				exactly(1).of(provider).readAttributes(path, CloudBasicFileAttributes.class);
				will(throwException(new FileNotFoundException()));
			} else {
				exactly(1).of(provider).readAttributes(path, CloudBasicFileAttributes.class);
				will(returnValue(basicAttributes));

				allowing(basicAttributes).getStorageType();
				will(returnValue(storageType));
			}
		}});

		return basicAttributes;
	}

	@Test
	public void testIsDirectoryReturnsFalseForAFileStorageType() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, StorageType.BLOB, cf.toPath());
		Assert.assertFalse(cf.isDirectory());
	}
	
	@Test
	public void testIsFileReturnsFalseForAContainer() throws IOException {
		final String container = "blah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container));
		mockStorageTypeAttributes(fs, StorageType.FOLDER, cf.toPath());
		Assert.assertFalse(cf.isFile());
	}

	
	@Test
	public void testIsFileReturnsFalseForADirectoryStorageType() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, StorageType.FOLDER, cf.toPath());
		Assert.assertFalse(cf.isFile());
	}

	@Test
	public void testIsFileReturnsFalseWhenTheBlobDoesntExist() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, null, cf.toPath());
		Assert.assertFalse(cf.isFile());
	}

	@Test
	public void testIsFileReturnsTrueForABlobStorageType() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah/plank.txt";
		
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, StorageType.BLOB, cf.toPath());
		Assert.assertTrue(cf.isFile());
	}
	
	@Test
	public void testLastModifiedReturnsZeroForAContainer() {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/container"));		
		Assert.assertEquals(0L, cf.lastModified());
	}

	@Test
	public void testLastModifiedReturnsZeroWhenTheBlobDoesntExist() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah";
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		mockStorageTypeAttributes(fs, null, cf.toPath());
		Assert.assertEquals(0L, cf.lastModified());
	}

	@Test
	public void testLastModifiedReturnsAValueForAFile() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah/plank.txt";
		long lastModified = 2378222L;
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		CloudBasicFileAttributes attributes = mockStorageTypeAttributes(fs, StorageType.BLOB, cf.toPath());
		final FileTime lastModifiedTime = FileTime.fromMillis(lastModified);

		context.checking(new Expectations() {{
			exactly(1).of(attributes).lastModifiedTime();
			will(returnValue(lastModifiedTime));
		}});

		Assert.assertEquals(lastModified, cf.lastModified());
	}

	@Test
	public void testLengthReturnsZeroForAContainer() {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/container"));		
		Assert.assertEquals(0L, cf.length());
	}

	@Test
	public void testLengthReturnsAValueForAFile() throws IOException {
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah/plank.txt";
		long length = 2378L;
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		CloudFile cf = new CloudFile(new CloudPath(fs, true, "/" + container + "/" + filePath));
		CloudBasicFileAttributes attributes = mockStorageTypeAttributes(fs, StorageType.BLOB, cf.toPath());

		context.checking(new Expectations() {{
			exactly(1).of(attributes).size();
			will(returnValue(length));
		}});

		Assert.assertEquals(length, cf.length());
	}
	
	@Test
	public void testSetWritableSetsOwnerOnlyRightsOnTheAclSet() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudPath cloudPath = new CloudPath(fs, true, "/container/path.txt");
		final CloudFile cf = new CloudFile(cloudPath);
		final CloudHostConfiguration hostConfig = context.mock(CloudHostConfiguration.class);
		final UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		final FileSystemProvider provider = context.mock(FileSystemProvider.class);
		final CloudFileAttributesView cloudFileAttributesView = context.mock(CloudFileAttributesView.class);
		final CloudAclFileAttributes cloudAclFileAttributes = context.mock(CloudAclFileAttributes.class);
		final UserPrincipal owner = new TestUserImpl("user1");
		final CloudAclEntrySet aclSet = new CloudAclEntrySet(owner);
		
		final CloudAclEntry<?> expectedEntry = new CloudAclEntryBuilder<>(UserPrincipal.class)
				.setPrincipal(owner)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS)
				.build();
		final CloudAclEntrySet expectedAclSet = new CloudAclEntrySet(owner, expectedEntry);

		context.checking(new Expectations() {{
			exactly(1).of(fs).getCloudHostConfiguration();
			will(returnValue(hostConfig));
			
			exactly(1).of(hostConfig).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			// Return the user which we will set the ACL with
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(owner));
			
			allowing(fs).provider();
			will(returnValue(provider));
			
			exactly(1).of(provider).getFileAttributeView(cloudPath, CloudFileAttributesView.class);
			will(returnValue(cloudFileAttributesView));
			
			allowing(cloudFileAttributesView).readAttributes();
			will(returnValue(cloudAclFileAttributes));
			
			allowing(cloudAclFileAttributes).getAclSet();
			will(returnValue(aclSet));
			
			exactly(1).of(cloudAclFileAttributes).getStorageType();
			will(returnValue(StorageType.BLOB));
			
			allowing(cloudAclFileAttributes).getOwners();
			will(returnValue(expectedAclSet.getOwners()));

			exactly(1).of(cloudFileAttributesView).setAclFileAttributes(with(equal(expectedAclSet)));
			will(returnValue(Collections.EMPTY_SET));
		}});
		

		Assert.assertTrue(cf.setWritable(true, true));
	}

	@Test
	public void testSetWritableSetsOwnerOnlyRightsForEachOwnerOnTheAclSet() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudPath cloudPath = new CloudPath(fs, true, "/container/path.txt");
		final CloudFile cf = new CloudFile(cloudPath);
		final CloudHostConfiguration hostConfig = context.mock(CloudHostConfiguration.class);
		final UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		final FileSystemProvider provider = context.mock(FileSystemProvider.class);
		final CloudFileAttributesView cloudFileAttributesView = context.mock(CloudFileAttributesView.class);
		final CloudAclFileAttributes cloudAclFileAttributes = context.mock(CloudAclFileAttributes.class);
		final UserPrincipal owner1 = new TestUserImpl("user1");
		final UserPrincipal owner2 = new TestUserImpl("user2");
		final CloudAclEntrySet aclSet = new CloudAclEntrySet(Sets.newHashSet(owner1, owner2));
		
		final CloudAclEntry<?> expectedEntry = new CloudAclEntryBuilder<>(UserPrincipal.class)
				.setPrincipal(owner1)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS)
				.build();
		final CloudAclEntry<?> expectedEntry2 = new CloudAclEntryBuilder<>(UserPrincipal.class)
				.setPrincipal(owner2)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS)
				.build();
		final CloudAclEntrySet expectedAclSet =
				new CloudAclEntrySet(Sets.newHashSet(owner1, owner2), Sets.newHashSet(expectedEntry, expectedEntry2));

		context.checking(new Expectations() {{
			exactly(1).of(fs).getCloudHostConfiguration();
			will(returnValue(hostConfig));
			
			exactly(1).of(hostConfig).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			// Return the user which we will set the ACL with
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(owner1));
			
			allowing(fs).provider();
			will(returnValue(provider));
			
			exactly(1).of(provider).getFileAttributeView(cloudPath, CloudFileAttributesView.class);
			will(returnValue(cloudFileAttributesView));
			
			allowing(cloudFileAttributesView).readAttributes();
			will(returnValue(cloudAclFileAttributes));
			
			allowing(cloudAclFileAttributes).getAclSet();
			will(returnValue(aclSet));
			
			exactly(1).of(cloudAclFileAttributes).getStorageType();
			will(returnValue(StorageType.BLOB));
			
			allowing(cloudAclFileAttributes).getOwners();
			will(returnValue(expectedAclSet.getOwners()));

			exactly(1).of(cloudFileAttributesView).setAclFileAttributes(with(equal(expectedAclSet)));
			will(returnValue(Collections.EMPTY_SET));
		}});
		

		Assert.assertTrue(cf.setWritable(true, true));
	}
	
	@Test
	public void testSetWriteableFailsToSetOwnerOnlyRightsOnTheAclSetWithAnAnonymousCurrentUser() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudPath cloudPath = new CloudPath(fs, true, "/container/path.txt");
		final CloudFile cf = new CloudFile(cloudPath);
		final CloudHostConfiguration hostConfig = context.mock(CloudHostConfiguration.class);
		final UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		final FileSystemProvider provider = context.mock(FileSystemProvider.class);
		final CloudFileAttributesView cloudFileAttributesView = context.mock(CloudFileAttributesView.class);
		final CloudAclFileAttributes cloudAclFileAttributes = context.mock(CloudAclFileAttributes.class);
		final UserPrincipal owner = new TestUserImpl("user1");
		final CloudAclEntrySet aclSet = new CloudAclEntrySet(owner);
		
		final CloudAclEntry<?> expectedEntry = new CloudAclEntryBuilder<>(UserPrincipal.class)
				.setPrincipal(owner)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS)
				.build();
		final CloudAclEntrySet expectedAclSet = new CloudAclEntrySet(owner, expectedEntry);

		context.checking(new Expectations() {{
			exactly(1).of(fs).getCloudHostConfiguration();
			will(returnValue(hostConfig));
			
			exactly(1).of(hostConfig).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			// Return the user which we will set the ACL with
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(null));
			
			allowing(fs).provider();
			will(returnValue(provider));
			
			exactly(1).of(provider).getFileAttributeView(cloudPath, CloudFileAttributesView.class);
			will(returnValue(cloudFileAttributesView));
			
			allowing(cloudFileAttributesView).readAttributes();
			will(returnValue(cloudAclFileAttributes));
			
			allowing(cloudAclFileAttributes).getAclSet();
			will(returnValue(aclSet));
			
			exactly(1).of(cloudAclFileAttributes).getStorageType();
			will(returnValue(StorageType.BLOB));
			
			allowing(cloudAclFileAttributes).getOwners();
			will(returnValue(expectedAclSet.getOwners()));
		}});
		

		Assert.assertFalse(cf.setWritable(true, true));
	}

	@Test
	public void testSetWritableSetsAnonymousOwnerOnlyRightsOnTheAclSetWithACurrentUser() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudPath cloudPath = new CloudPath(fs, true, "/container/path.txt");
		final CloudFile cf = new CloudFile(cloudPath);
		final CloudHostConfiguration hostConfig = context.mock(CloudHostConfiguration.class);
		final UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		final FileSystemProvider provider = context.mock(FileSystemProvider.class);
		final CloudFileAttributesView cloudFileAttributesView = context.mock(CloudFileAttributesView.class);
		final CloudAclFileAttributes cloudAclFileAttributes = context.mock(CloudAclFileAttributes.class);
		final UserPrincipal owner = new TestUserImpl("user1");
		final CloudAclEntrySet aclSet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		
		// The principal is the owner(s) of the ACL even though we attempt to set the ACL with the owner
		// supplied
		final CloudAclEntry<?> expectedEntry = new CloudAclEntryBuilder<>(UserPrincipal.class)
				.setPrincipal(AnonymousUserPrincipal.INSTANCE)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS)
				.build();
		final CloudAclEntrySet expectedAclSet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, expectedEntry);

		context.checking(new Expectations() {{
			exactly(1).of(fs).getCloudHostConfiguration();
			will(returnValue(hostConfig));
			
			exactly(1).of(hostConfig).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			// Return the user which we will set the ACL with
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(owner));
			
			allowing(fs).provider();
			will(returnValue(provider));
			
			exactly(1).of(provider).getFileAttributeView(cloudPath, CloudFileAttributesView.class);
			will(returnValue(cloudFileAttributesView));
			
			allowing(cloudFileAttributesView).readAttributes();
			will(returnValue(cloudAclFileAttributes));
			
			allowing(cloudAclFileAttributes).getAclSet();
			will(returnValue(aclSet));
			
			exactly(1).of(cloudAclFileAttributes).getStorageType();
			will(returnValue(StorageType.BLOB));
			
			allowing(cloudAclFileAttributes).getOwners();
			will(returnValue(expectedAclSet.getOwners()));

			exactly(1).of(cloudFileAttributesView).setAclFileAttributes(with(equal(expectedAclSet)));
			will(returnValue(Collections.EMPTY_SET));
		}});
		

		Assert.assertTrue(cf.setWritable(true, true));
	}


	@Test
	public void testSetWritableSetsAnonymousOwnerOnlyRightsOnTheAclSetWithNoCurrentUser() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudPath cloudPath = new CloudPath(fs, true, "/container/path.txt");
		final CloudFile cf = new CloudFile(cloudPath);
		final CloudHostConfiguration hostConfig = context.mock(CloudHostConfiguration.class);
		final UserGroupLookupService<?> lookupService = context.mock(UserGroupLookupService.class);
		final FileSystemProvider provider = context.mock(FileSystemProvider.class);
		final CloudFileAttributesView cloudFileAttributesView = context.mock(CloudFileAttributesView.class);
		final CloudAclFileAttributes cloudAclFileAttributes = context.mock(CloudAclFileAttributes.class);
		final CloudAclEntrySet aclSet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE);
		
		// The principal is the owner(s) of the ACL even though we attempt to set the ACL with the owner
		// supplied
		final CloudAclEntry<?> expectedEntry = new CloudAclEntryBuilder<>(UserPrincipal.class)
				.setPrincipal(AnonymousUserPrincipal.INSTANCE)
				.setType(AclEntryType.ALLOW)
				.addPermissions(AclConstants.ALL_FILE_WRITE_PERMISSIONS)
				.build();
		final CloudAclEntrySet expectedAclSet = new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, expectedEntry);

		context.checking(new Expectations() {{
			exactly(1).of(fs).getCloudHostConfiguration();
			will(returnValue(hostConfig));
			
			exactly(1).of(hostConfig).getUserGroupLookupService();
			will(returnValue(lookupService));
			
			// Return the user which we will set the ACL with
			exactly(1).of(lookupService).getCurrentUser();
			will(returnValue(null));
			
			allowing(fs).provider();
			will(returnValue(provider));
			
			exactly(1).of(provider).getFileAttributeView(cloudPath, CloudFileAttributesView.class);
			will(returnValue(cloudFileAttributesView));
			
			allowing(cloudFileAttributesView).readAttributes();
			will(returnValue(cloudAclFileAttributes));
			
			allowing(cloudAclFileAttributes).getAclSet();
			will(returnValue(aclSet));
			
			exactly(1).of(cloudAclFileAttributes).getStorageType();
			will(returnValue(StorageType.BLOB));
			
			allowing(cloudAclFileAttributes).getOwners();
			will(returnValue(expectedAclSet.getOwners()));

			exactly(1).of(cloudFileAttributesView).setAclFileAttributes(with(equal(expectedAclSet)));
			will(returnValue(Collections.EMPTY_SET));
		}});
		

		Assert.assertTrue(cf.setWritable(true, true));
	}

}
