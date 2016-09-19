package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntryType;
import java.security.acl.NotOwnerException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jclouds.blobstore.domain.BlobAccess;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.MediaType;
import com.uk.xarixa.cloud.filesystem.core.AbstractJCloudsIntegrationTest;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.CloudPermissionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentDispositionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentEncodingFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentLanguageFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentTypeFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.UserDefinedFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation.CloudMethod;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.CloudFileChannel;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.DefaultCloudFileChannelTransport;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.DefaultCloudAclEntryConflictChecker;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.PublicPrivateCloudPermissionsPrincipal;
import com.uk.xarixa.cloud.filesystem.core.nio.options.CloudCopyOption;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;

@RunWith(BlockJUnit4ClassRunner.class)
public class DefaultCloudFileSystemImplementationIntegrationTest extends AbstractJCloudsIntegrationTest {
	private DefaultCloudFileSystemImplementation impl;

	@Override
	protected void postSetUp() {
		impl = new DefaultCloudFileSystemImplementation();
	}
	
	@Test
	public void testNewByteChannelCreatesACloudFileChannelWithDefaultInterceptor() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = new CloudPath(containerPath, pathName);
		CloudFileChannel cloudFileChannel =
				impl.newByteChannel(blobStoreContext, cloudPath, EnumSet.of(StandardOpenOption.READ));
		try {
			Assert.assertEquals(DefaultCloudFileChannelTransport.class, cloudFileChannel.getTransport().getClass());
		} finally {
			cloudFileChannel.close();
		}
	}

	@Test
	public void testFileAttributesAreSetWhenAFileIsCreated() throws IOException {
		Date currentTime = new Date();
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		CloudPath cloudPath = new CloudPath(containerPath, pathName);
		createDirectory(cloudPath.getParent());

		// Write content to the channel
		CloudFileChannel channel = impl.newByteChannel(blobStoreContext, cloudPath,
				EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
				new CloudPermissionFileAttribute<>(BlobAccess.PUBLIC_READ),
				new ContentEncodingFileAttribute("UTF-8"),
				new ContentTypeFileAttribute(MediaType.PLAIN_TEXT_UTF_8),
				new ContentLanguageFileAttribute("en-gb"));
		try {
			channel.write(ByteBuffer.wrap(originalContent.getBytes("UTF-8")));
		} finally {
			channel.close();
		}

		// Now read the attributes
		CloudBasicFileAttributes attributes = impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, cloudPath);
		Assert.assertTrue("File time is out: " + attributes.creationTime(),
				currentTime.getTime() - 60000 <= attributes.creationTime().toMillis());
		Assert.assertTrue(attributes.creationTime().equals(attributes.lastModifiedTime()));
		Assert.assertTrue(attributes.isRegularFile());
		Assert.assertFalse(attributes.isContainer());
		Assert.assertFalse(attributes.isDirectory());
		Assert.assertFalse(attributes.isSymbolicLink());
		Assert.assertFalse(attributes.isOther());
		Assert.assertEquals(originalContent.length(), attributes.size());
		Assert.assertNotNull(attributes.getContentMD5());
		Assert.assertNotNull(attributes.getETag());
		Assert.assertNotNull(attributes.getUri());
		Assert.assertTrue(attributes.getUserMetadata().isEmpty());
		Assert.assertEquals("en-gb", attributes.getContentLanguage());
		Assert.assertEquals("text/plain", attributes.getContentType());
		Assert.assertEquals("UTF-8", attributes.getContentEncoding());
		Assert.assertNull(attributes.getContentDisposition());
	}

	@Test
	public void testFileAttributesAreSetWhenAFileIsCreatedWithEncodingFromTheMediaType() throws IOException {
		Date currentTime = new Date();
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		String contentDisposition = "attachment; filename=sushi.jpg";
		CloudPath cloudPath = new CloudPath(containerPath, pathName);
		createDirectory(cloudPath.getParent());

		// Write content to the channel
		CloudFileChannel channel = impl.newByteChannel(blobStoreContext, cloudPath,
				EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
				new CloudPermissionFileAttribute<>(BlobAccess.PUBLIC_READ),
				// new ContentEncodingFileAttribute("UTF-8"), 		<- Content encoding should be applied in the next line
				new ContentTypeFileAttribute(MediaType.PLAIN_TEXT_UTF_8),
				new ContentLanguageFileAttribute("en-gb"),
				new ContentDispositionFileAttribute(contentDisposition));
		try {
			channel.write(ByteBuffer.wrap(originalContent.getBytes("UTF-8")));
		} finally {
			channel.close();
		}

		// Now read the attributes
		CloudBasicFileAttributes attributes = impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, cloudPath);
		Assert.assertTrue("File time is out: " + attributes.creationTime(),
				currentTime.getTime() - 60000 <= attributes.creationTime().toMillis());
		Assert.assertTrue(attributes.creationTime().equals(attributes.lastModifiedTime()));
		Assert.assertTrue(attributes.isRegularFile());
		Assert.assertFalse(attributes.isContainer());
		Assert.assertFalse(attributes.isDirectory());
		Assert.assertFalse(attributes.isSymbolicLink());
		Assert.assertFalse(attributes.isOther());
		Assert.assertEquals(originalContent.length(), attributes.size());
		Assert.assertNotNull(attributes.getContentMD5());
		Assert.assertNotNull(attributes.getETag());
		Assert.assertNotNull(attributes.getUri());
		Assert.assertTrue(attributes.getUserMetadata().isEmpty());
		Assert.assertEquals("en-gb", attributes.getContentLanguage());
		Assert.assertEquals("text/plain", attributes.getContentType());
		Assert.assertEquals("UTF-8", attributes.getContentEncoding());
		Assert.assertEquals(contentDisposition, attributes.getContentDisposition());
	}

	@Test
	public void testCreateDirectoryCreatesADirectoryMarker() throws IOException {
		String pathName = "cloud-dir";
		CloudPath cloudPath = new CloudPath(containerPath, pathName);
		createDirectory(cloudPath);
	}

	@Test
	public void testCreateDirectoryCreatesAContainer() throws IOException {
		String containerName = CONTAINER_NAME + "-2";
		CloudPath cloudPath = new CloudPath(fileSystem, true, containerName);
		
		try {
			createDirectory(cloudPath);
		} finally {
			blobStoreContext.getBlobStore().deleteContainer(containerName);
		}
	}

	@Test
	public void testCreateDirectoryFailsIfTheParentDirectoryDoesNotExist() throws IOException {
		String pathName = "content/cloud-dir";
		CloudPath cloudPath = new CloudPath(containerPath, pathName);

		try {
			impl.createDirectory(blobStoreContext, cloudPath);
			Assert.fail("Expected an exception to be thrown");
		} catch (IOException e) {
			// OK
		}

		assertNotExists(cloudPath);
	}

	@Test
	public void testCreateDirectoryFailsIfTheDirectoryAlreadyExists() throws IOException {
		String pathName = "content/cloud-dir";
		CloudPath cloudPath = new CloudPath(containerPath, pathName);
		
		// Create the parent dir and dir
		createDirectory(cloudPath.getParent());
		createDirectory(cloudPath);
		assertDirectoryExists(cloudPath);

		// Now issue another create
		try {
			impl.createDirectory(blobStoreContext, cloudPath);
			Assert.fail("Expected an exception to be thrown");
		} catch (FileAlreadyExistsException e) {
			// OK
		}
	}

	@Test
	public void testDeleteWillDeleteAContainer() throws IOException {
		Assert.assertTrue(blobStoreContext.getBlobStore().containerExists(CONTAINER_NAME));
		impl.delete(blobStoreContext, containerPath, EnumSet.noneOf(DeleteOption.class));
		Assert.assertFalse(blobStoreContext.getBlobStore().containerExists(CONTAINER_NAME));
	}

	@Test
	public void testDeleteWillDeleteADirectoryMarker() throws IOException {
		String dirName = "cloud-dir";
		CloudPath dirPath = new CloudPath(containerPath, dirName);

		// Crate the directory
		impl.createDirectory(blobStoreContext, dirPath);
		CloudBasicFileAttributes attributes = impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, dirPath);
		Assert.assertTrue(attributes.isDirectory());

		// Now delete the directory
		impl.delete(blobStoreContext, dirPath, EnumSet.noneOf(DeleteOption.class));
		assertNotExists(dirPath);
	}

	@Test
	public void testDeleteWillNotDeleteADirectoryRecursively() throws IOException {
		String originalContent = "This is my content";

		// Create a dir
		CloudPath dirPath = new CloudPath(containerPath, "subdir");
		createDirectory(dirPath);

		// Create content in this dir
		String contentPathFilename = "subdir/cloud-file-channel-test.txt";
		CloudPath contentPath1 = new CloudPath(dirPath, contentPathFilename);
		createRawContent(contentPath1.getPathName(), originalContent.getBytes("UTF-8"));
		assertFileExists(contentPath1);

		// Create subdir
		CloudPath dirPath2 = new CloudPath(containerPath, "subdir/subdir2");
		impl.createDirectory(blobStoreContext, dirPath2);
		assertDirectoryExists(dirPath);

		// Create content in a subdir
		String contentPathFilename2 = "cloud-file-channel-test.txt";
		CloudPath contentPath2 = new CloudPath(dirPath2, contentPathFilename2);
		createRawContent(contentPath2.getPathName(), originalContent.getBytes("UTF-8"));
		assertFileExists(contentPath2);
		
		// Create a control file outside of the main directory which should exist after the delete
		String controlFilePathString = "control-file-test.txt";
		CloudPath controlFilePath = new CloudPath(dirPath, controlFilePathString);
		createRawContent(controlFilePath.getPathName(), originalContent.getBytes("UTF-8"));
		assertFileExists(controlFilePath);

		// Perform delete
		try {
			impl.delete(blobStoreContext, dirPath, EnumSet.noneOf(DeleteOption.class));
			Assert.fail("Did not expect to be able to delete a non-empty directory");
		} catch (DirectoryNotEmptyException e) {
			// OK
		}
	}

	@Test
	public void testDeleteWillDeleteAnEmptyDirectory() throws IOException {
		// Create a dir
		CloudPath dirPath = new CloudPath(containerPath, "subdir");
		createDirectory(dirPath);

		// Perform delete
		impl.delete(blobStoreContext, dirPath, EnumSet.noneOf(DeleteOption.class));
		assertNotExists(dirPath);
	}

	@Test
	public void testDeleteWithRecursiveDeleteOptionWillDeleteADirectoryRecursively() throws IOException {		
		String originalContent = "This is my content";

		// Create a dir
		CloudPath dirPath = new CloudPath(containerPath, "subdir");
		impl.createDirectory(blobStoreContext, dirPath);
		assertDirectoryExists(dirPath);

		// Create content in this dir
		String contentPathString1 = "cloud-file-channel-test.txt";
		CloudPath contentPath1 = new CloudPath(dirPath, contentPathString1);
		createRawContent(contentPath1.getPathName(), originalContent.getBytes("UTF-8"));
		assertFileExists(contentPath1);

		// Create subdir
		CloudPath dirPath2 = new CloudPath(containerPath, "subdir/subdir2");
		impl.createDirectory(blobStoreContext, dirPath2);
		assertDirectoryExists(dirPath);

		// Create content in subdir/subdir2
		String contentPathString2 = "cloud-file-channel-test.txt";
		CloudPath contentPath2 = new CloudPath(dirPath2, contentPathString2);
		createRawContent(contentPath2.getPathName(), originalContent.getBytes("UTF-8"));
		assertFileExists(contentPath2);
		
		// Create a control file outside of the main directory which should exist after the delete
		String controlFilePathString = "control-file-test.txt";
		CloudPath controlFilePath = new CloudPath(containerPath, controlFilePathString);
		createRawContent(controlFilePath.getPathName(), originalContent.getBytes("UTF-8"));
		assertFileExists(controlFilePath);

		// Perform delete
		impl.delete(blobStoreContext, dirPath, EnumSet.of(DeleteOption.RECURSIVE));

		// Ensure the directory is deleted
		assertNotExists(dirPath);
		assertNotExists(contentPath1);
		assertNotExists(dirPath2);
		assertNotExists(contentPath2);
		assertFileExists(controlFilePath);
	}

	@Test
	public void testNewDirectoryStreamListsContentsOfADirectoryOnly() throws IOException {
		// The following files/dirs are created under the container:
		//	cloud-file-channel-test.txt
		//	cloud-file-channel-test-2.txt
		//	subdir/
		//	subdir/cloud-file-channel-test-3.txt
		//	subdir/subdir2/cloud-file-channel-test-4.txt
		String originalContent = "This is my content";
		String testFile1 = "cloud-file-channel-test.txt";
		createRawContent(testFile1, originalContent.getBytes("UTF-8"));
		String testFile2 = "cloud-file-channel-test-2.txt";
		createRawContent(testFile2, originalContent.getBytes("UTF-8"));
		CloudPath subdir = new CloudPath(containerPath, "subdir");
		createDirectory(subdir);
		String testFile3 = "subdir/cloud-file-channel-test-3.txt";
		createRawContent(testFile3, originalContent.getBytes("UTF-8"));
		CloudPath subdir2 = new CloudPath(containerPath, "subdir/subdir2");
		createDirectory(subdir2);
		String testFile4 = "subdir/subdir2/cloud-file-channel-test-4.txt";
		CloudPath cloudPath4 = new CloudPath(containerPath, testFile4);
		createRawContent(testFile4, originalContent.getBytes("UTF-8"));
		CloudPath nonExistantPath = new CloudPath(containerPath, "subdir/subdir2/subdir3");
		
		Filter<CloudPath> acceptAllFilter = new Filter<CloudPath>() {
			@Override
			public boolean accept(CloudPath entry) throws IOException {
				return true;
			}
		};

		// List contents of the container
		ArrayList<CloudPath> dirContents = Lists.newArrayList(
				impl.newDirectoryStream(blobStoreContext, containerPath, acceptAllFilter).iterator());
		Assert.assertEquals("Expected some contents in " + containerPath + ", but was: " + directoryContentsToString(null),
				3, dirContents.size());

		// List contents of subdir
		dirContents = Lists.newArrayList(
				impl.newDirectoryStream(blobStoreContext, subdir, acceptAllFilter).iterator());
		Assert.assertEquals("Not the number of expected elements in: " + dirContents, 2, dirContents.size());

		// List contents of subdir2
		dirContents = Lists.newArrayList(
				impl.newDirectoryStream(blobStoreContext, subdir2, acceptAllFilter).iterator());
		Assert.assertEquals("Not the number of expected elements in: " + dirContents, 1, dirContents.size());

		// List contents by an existing file path, which should list by the parent directory
		dirContents = Lists.newArrayList(
				impl.newDirectoryStream(blobStoreContext, cloudPath4, acceptAllFilter).iterator());
		Assert.assertEquals("Not the number of expected elements in: " + dirContents, 1, dirContents.size());

		// List by non-existant path
		try {
			impl.newDirectoryStream(blobStoreContext, nonExistantPath, acceptAllFilter);
			Assert.fail("Did not expect to be able to find non-existant directory " + nonExistantPath.toString());
		} catch (FileNotFoundException e) {
			// OK
		}
	}

	@Test
	public void testDefaultCopyToANewFileWillUseOptimisedCopy() throws IOException {
		String originalContent = "This is my content";

		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		Map<String,String> userDefinedMap = new HashMap<>();
		userDefinedMap.put("key1", "value1");
		userDefinedMap.put("key2", "value2");

		// Create a file with storage options
		try ( CloudFileChannel channel = impl.newByteChannel(blobStoreContext, testFilePath,
				EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
				new CloudPermissionFileAttribute<>(BlobAccess.PUBLIC_READ),
				new ContentDispositionFileAttribute("attachment; filename=sushi.jpg"),
				new ContentEncodingFileAttribute("UTF-8"),
				new ContentLanguageFileAttribute("en-GB"),
				new ContentTypeFileAttribute(MediaType.JPEG),
				new UserDefinedFileAttributes(userDefinedMap))) {
			channel.write(ByteBuffer.wrap(originalContent.getBytes("UTF-8")));
		}
		// createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFilePath);

		String targetFileName = "target-copy.txt";
		CloudPath targetFilePath = new CloudPath(containerPath, targetFileName);

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.copy(blobStoreContext, testFilePath, targetFilePath, Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES)));

		// Assert the content was saved and is the same in the target as the source
		List<String> targetContent = getContentAsLines(targetFileName);
		Assert.assertEquals(1, targetContent.size());
		Assert.assertEquals(originalContent, targetContent.get(0));

		// Read attributes back to make sure the file is there
		CloudAclFileAttributes attrs = impl.readAttributes(blobStoreContext, CloudAclFileAttributes.class, targetFilePath);
		Assert.assertEquals("attachment; filename=sushi.jpg", attrs.getContentDisposition());
		Assert.assertEquals(MediaType.JPEG.toString(), attrs.getContentType());
		Assert.assertEquals("UTF-8", attrs.getContentEncoding());
		Assert.assertEquals("en-GB", attrs.getContentLanguage());
		Assert.assertEquals(userDefinedMap, attrs.getUserMetadata());

		// Assert ACL's
		CloudAclEntrySet aclSet = attrs.getAclSet();
		Assert.assertEquals(1, aclSet.size());
		CloudAclEntry<?> entry = aclSet.iterator().next();
		Assert.assertEquals(PublicPrivateCloudPermissionsPrincipal.class, entry.getPrincipalClass());
		Assert.assertEquals(BlobAccess.PUBLIC_READ, ((PublicPrivateCloudPermissionsPrincipal)entry.getPrincipal()).getBlobAccess());
	}
	
	@Test
	public void testDefaultCopyOfAnEmptyDirectoryWillCreateANewDirectory() throws IOException {
		CloudPath dir1 = new CloudPath(containerPath, "subdir1");
		createDirectory(dir1);

		CloudPath dir2 = new CloudPath(containerPath, "subdir2");

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.copy(blobStoreContext, dir1, dir2, Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES)));
		// Read attributes back to make sure the file is there
		CloudBasicFileAttributes targetAttributes = impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, dir2);
		Assert.assertTrue(targetAttributes.isDirectory());
	}

	@Test
	public void testDefaultCopyWillFailIfTheTargetExistsAndReplaceExistingCopyOptionIsNotUsed() throws IOException {
		String originalContent = "This is my content";
		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFilePath);

		String targetFileName = "target-copy.txt";
		CloudPath targetFilePath = new CloudPath(containerPath, targetFileName);
		createRawContent(targetFileName, BlobAccess.PRIVATE, originalContent.getBytes("UTF-8"));
		assertFileExists(targetFilePath);

		try {
			impl.copy(blobStoreContext, testFilePath, targetFilePath, Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES));
			Assert.fail("Did not expect to be able to copy");
		} catch (FileAlreadyExistsException e) {
			// OK
		}
	}

	// TODO: Disabled until CloudFileSystemProviderDelegate has been completed
	//	@Test
	public void testLocalFilesystemCopyWillFailIfTheTargetExistsAndReplaceExistingCopyOptionIsNotUsed() throws IOException {
		String originalContent = "This is my content";
		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		String targetFileName = "target-copy.txt";
		CloudPath targetFilePath = new CloudPath(containerPath, targetFileName);
		createRawContent(targetFileName, BlobAccess.PRIVATE, originalContent.getBytes("UTF-8"));

		try {
			impl.copyUsingLocalFilesystem(blobStoreContext, testFilePath, targetFilePath,
					Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES));
			Assert.fail("Did not expect to be able to copy");
		} catch (FileAlreadyExistsException e) {
			// OK
		}
	}

	@Test
	public void testDefaultCopyWillOverwriteAnExistingFileIfTheReplaceOptionIsSet() throws IOException {
		String originalContent = "This is my content";
		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFilePath);

		String targetFileName = "target-copy.txt";
		CloudPath targetFilePath = new CloudPath(containerPath, targetFileName);
		createRawContent(targetFileName, BlobAccess.PRIVATE, originalContent.getBytes("UTF-8"));
		assertFileExists(testFilePath);

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.copy(blobStoreContext, testFilePath, targetFilePath,
					Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING)));
		// Read attributes back to make sure the file is there
		CloudBasicFileAttributes targetAttributes =
				impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, targetFilePath);
		List<String> targetContent = getContentAsLines(targetFileName);
		Assert.assertEquals(1, targetContent.size());
		Assert.assertEquals(originalContent, targetContent.get(0));
	}

	@Test
	public void testDefaultMoveWillOverwriteAnExistingFileIfTheReplaceOptionIsSet() throws IOException {
		String originalContent = "This is my content";

		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		String targetFileName = "target-copy.txt";
		CloudPath targetFilePath = new CloudPath(containerPath, targetFileName);
		createRawContent(targetFileName, BlobAccess.PRIVATE, originalContent.getBytes("UTF-8"));

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.move(blobStoreContext, testFilePath, targetFilePath,
					Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING)));
		// Read attributes back to make sure the file is there
		CloudBasicFileAttributes targetAttributes = impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, targetFilePath);
		List<String> targetContent = getContentAsLines(targetFileName);
		Assert.assertEquals(1, targetContent.size());
		Assert.assertEquals(originalContent, targetContent.get(0));
		assertNotExists(testFilePath);
	}

	@Test
	public void testDefaultCopyOfADirectoryWillNotCopyANonEmptyDirectoryRecursivelyWithoutTheRecursiveCopyOption() throws IOException {
		String originalContent = "This is my content";

		CloudPath dir1 = new CloudPath(containerPath, "subdir1");
		impl.createDirectory(blobStoreContext, dir1);

		String testFileName = "subdir1/cloud-file-channel-test.txt";
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		String testFileName2 = "subdir1/cloud-file-channel-test2.txt";
		createRawContent(testFileName2, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		CloudPath dir2 = new CloudPath(containerPath, "subdir1/subdir2");
		impl.createDirectory(blobStoreContext, dir2);

		String testFileName3 = "subdir1/subdir2/cloud-file-channel-test3.txt";
		createRawContent(testFileName3, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		String targetDirName = "targetDir";
		CloudPath targetDir = new CloudPath(containerPath, targetDirName);

		impl.copy(blobStoreContext, dir1, targetDir, Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES));
		assertDirectoryExists(targetDir);
		assertNotExists(targetDirName + "/cloud-file-channel-test.txt");
		assertNotExists(targetDirName + "/cloud-file-channel-test2.txt");
		assertNotExists(targetDirName + "/subdir2");
		assertNotExists(targetDirName + "/subdir2/cloud-file-channel-test3.txt");
	}

	@Test
	public void testDefaultCopyOfANonEmptyDirectoryWillCopyTheDirectoryRecursively() throws IOException {
		String originalContent = "This is my content";
		String targetDirName = "target-dir";
		CloudPath targetDir = new CloudPath(containerPath, targetDirName);
		assertNotExists(targetDir);

		CloudPath dir1 = new CloudPath(containerPath, "subdir1");
		createDirectory(dir1);

		String testFileName = "subdir1/cloud-file-channel-test.txt";
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName);

		String testFileName2 = "subdir1/cloud-file-channel-test2.txt";
		createRawContent(testFileName2, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName2);

		CloudPath dir2 = new CloudPath(containerPath, "subdir1/subdir2");
		createDirectory(dir2);

		String testFileName3 = "subdir1/subdir2/cloud-file-channel-test3.txt";
		createRawContent(testFileName3, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName3);

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.copy(blobStoreContext, dir1, targetDir,
					Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, CloudCopyOption.RECURSIVE)));
		// Read attributes back to make sure the folder structure exists in the target
		assertDirectoryExists(targetDir);
		assertFileExists(targetDirName + "/cloud-file-channel-test.txt");
		assertFileExists(targetDirName + "/cloud-file-channel-test2.txt");
		assertDirectoryExists(targetDirName + "/subdir2");
		assertFileExists(targetDirName + "/subdir2/cloud-file-channel-test3.txt");
	}

	void createDirectory(CloudPath dir) throws IOException {
		impl.createDirectory(blobStoreContext, dir);
		assertDirectoryExists(dir);
	}

	@Test
	public void testDefaultMoveOfADirectoryWillMoveTheDirectoryRecursively() throws IOException {
		String originalContent = "This is my content";

		CloudPath dir1 = new CloudPath(containerPath, "subdir1");
		impl.createDirectory(blobStoreContext, dir1);
		assertDirectoryExists(dir1);

		String testFileName = "subdir1/cloud-file-channel-test.txt";
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName);

		String testFileName2 = "subdir1/cloud-file-channel-test2.txt";
		createRawContent(testFileName2, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName2);

		CloudPath dir2 = new CloudPath(containerPath, "subdir1/subdir2");
		impl.createDirectory(blobStoreContext, dir2);
		assertDirectoryExists(dir2);

		String testFileName3 = "subdir1/subdir2/cloud-file-channel-test3.txt";
		createRawContent(testFileName3, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName3);

		String targetDirName = "targetDir";
		CloudPath targetDir = new CloudPath(containerPath, targetDirName);
		assertNotExists(targetDir);

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.move(blobStoreContext, dir1, targetDir,
					Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, CloudCopyOption.RECURSIVE)));
		// Read attributes back to make sure the folder structure exists in the target
		assertDirectoryExists(targetDir);
		assertFileExists(targetDirName + "/cloud-file-channel-test.txt");
		assertFileExists(targetDirName + "/cloud-file-channel-test2.txt");
		assertDirectoryExists(targetDirName + "/subdir2");
		assertFileExists(targetDirName + "/subdir2/cloud-file-channel-test3.txt");
		
		// These should now not exist
		assertNotExists(dir1);
		assertNotExists(testFileName);
		assertNotExists(testFileName2);
		assertNotExists(dir2);
		assertNotExists(testFileName3);
	}

	@Test
	public void testDefaultCopyOfADirectoryWillFailIfTheTargetDirectoryIsNotEmptyAndTheRecursiveOptionHasNotBeenSpecified() throws IOException {
		String originalContent = "This is my content";

		CloudPath dir1 = new CloudPath(containerPath, "subdir1");
		impl.createDirectory(blobStoreContext, dir1);
		assertDirectoryExists(dir1);

		String testFileName = "subdir1/cloud-file-channel-test.txt";
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName);

		String testFileName2 = "subdir1/cloud-file-channel-test2.txt";
		createRawContent(testFileName2, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName2);

		CloudPath dir2 = new CloudPath(containerPath, "subdir1/subdir2");
		impl.createDirectory(blobStoreContext, dir2);
		assertDirectoryExists(dir2);

		String testFileName3 = "subdir1/subdir2/cloud-file-channel-test3.txt";
		createRawContent(testFileName3, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName3);

		String targetDirName = "targetDir";
		CloudPath targetDir = new CloudPath(containerPath, targetDirName);
		impl.createDirectory(blobStoreContext, targetDir);
		assertDirectoryExists(targetDir);

		String testFileName4 = "targetDir/cloud-file-channel-test3.txt";
		createRawContent(testFileName4, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName4);

		try {
			impl.copy(blobStoreContext, dir1, targetDir,
				Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING));
			Assert.fail("Did not expect the copy to succeed");
		} catch (DirectoryNotEmptyException e) {
			// OK
		}
	}

	@Test
	public void testDefaultCopyOfADirectoryWillSucceedIfTheTargetDirectoryIsNotEmptyAndTheRecursiveOptionHasBeenSpecified() throws IOException {
		String originalContent = "This is my content";

		CloudPath dir1 = new CloudPath(containerPath, "subdir1");
		impl.createDirectory(blobStoreContext, dir1);
		assertDirectoryExists(dir1);

		String testFileName = "subdir1/cloud-file-channel-test.txt";
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName);

		String testFileName2 = "subdir1/cloud-file-channel-test2.txt";
		createRawContent(testFileName2, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName2);

		CloudPath dir2 = new CloudPath(containerPath, "subdir1/subdir2");
		impl.createDirectory(blobStoreContext, dir2);
		assertDirectoryExists(dir2);

		String testFileName3 = "subdir1/subdir2/cloud-file-channel-test3.txt";
		createRawContent(testFileName3, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName3);

		String targetDirName = "targetDir";
		CloudPath targetDir = new CloudPath(containerPath, targetDirName);
		impl.createDirectory(blobStoreContext, targetDir);
		assertDirectoryExists(targetDir);

		String testFileName4 = "targetDir/cloud-file-channel-test3.txt";
		createRawContent(testFileName4, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));
		assertFileExists(testFileName4);

		Assert.assertEquals(CloudMethod.CLOUD_OPTIMISED, 
				impl.copy(blobStoreContext, dir1, targetDir,
					Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING, CloudCopyOption.RECURSIVE)));
		// Read attributes back to make sure the folder structure exists in the target
		assertDirectoryExists(targetDir);
		assertFileExists(targetDirName + "/cloud-file-channel-test.txt");
		assertFileExists(targetDirName + "/cloud-file-channel-test2.txt");
		assertDirectoryExists(targetDirName + "/subdir2");
		assertFileExists(targetDirName + "/subdir2/cloud-file-channel-test3.txt");
	}

	// TODO: Disabled until CloudFileSystemProviderDelegate has been completed
	//	@Test
	public void testLocalFilesystemCopyWillOverwriteAnExistingFileIfTheCopyOptionIsSet() throws IOException {
		String originalContent = "This is my content";

		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		String targetFileName = "target-copy.txt";
		CloudPath targetFilePath = new CloudPath(containerPath, targetFileName);
		createRawContent(targetFileName, BlobAccess.PRIVATE, originalContent.getBytes("UTF-8"));

		impl.copyUsingLocalFilesystem(blobStoreContext, testFilePath, targetFilePath,
				Sets.newHashSet(StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING));

		// Read attributes back to make sure the file is there
		CloudAclFileAttributes targetAttributes = impl.readAttributes(blobStoreContext, CloudAclFileAttributes.class, targetFilePath);
		Set<CloudAclEntry<PublicPrivateCloudPermissionsPrincipal>> cloudAclEntries =
				targetAttributes.getAclSet().findAclsWithPrincipalType(PublicPrivateCloudPermissionsPrincipal.class);
		Assert.assertEquals(1, cloudAclEntries.size());
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> cloudAclEntry = cloudAclEntries.iterator().next();
		Assert.assertEquals(PublicPrivateCloudPermissionsPrincipal.class, cloudAclEntry.getPrincipal().getClass());
		Assert.assertEquals(BlobAccess.PUBLIC_READ, ((PublicPrivateCloudPermissionsPrincipal)cloudAclEntry.getPrincipal()).getBlobAccess());
		List<String> targetContent = getContentAsLines(targetFileName);
		Assert.assertEquals(1, targetContent.size());
		Assert.assertEquals(originalContent, targetContent.get(0));
	}

	@Test
	public void testReadAttributesReadsDefaultAttributes() throws IOException {
		String originalContent = "This is some content";
		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		// Read the basic attributes
		Map<String, Object> readAttributes = impl.readAttributes(blobStoreContext, testFilePath, "*");
		Assert.assertNotNull(readAttributes.get("lastAccessTime"));
		Assert.assertNotNull(readAttributes.get("lastModifiedTime"));
		Assert.assertNotNull(readAttributes.get("creationTime"));
		Assert.assertNotNull(readAttributes.get("contentMD5"));
		Assert.assertNotNull(readAttributes.get("contentType"));
		Assert.assertNotNull(readAttributes.get("eTag"));
		Assert.assertNotNull(readAttributes.get("uri"));
		Assert.assertNotNull(readAttributes.get("userMetadata"));
		Assert.assertNotNull(readAttributes.get("aclSet"));

		// Read the ACL's
		CloudAclEntrySet cloudAclEntrySet = (CloudAclEntrySet)readAttributes.get("aclSet");
		assertPublicPrivateAccessAcl(cloudAclEntrySet, BlobAccess.PUBLIC_READ);
	}

	@Test
	public void testCloudFileAttributesViewAllowsThePublicPrivateAccessAclToBeModified() throws IOException, NotOwnerException {
		String originalContent = "This is some content";
		String testFileName = "cloud-file-channel-test.txt";
		CloudPath testFilePath = new CloudPath(containerPath, testFileName);
		createRawContent(testFileName, BlobAccess.PUBLIC_READ, originalContent.getBytes("UTF-8"));

		// Get the view
		CloudFileAttributesView fileAttributeView =
				impl.getFileAttributeView(blobStoreContext, CloudFileAttributesView.class, testFilePath);

		// Read the ACL's
		CloudAclFileAttributes readAclFileAttributes = fileAttributeView.readAttributes();
		CloudAclEntrySet cloudAclEntrySet = readAclFileAttributes.getAclSet();
		assertPublicPrivateAccessAcl(cloudAclEntrySet, BlobAccess.PUBLIC_READ);
		
		// Change the access
		CloudAclEntrySet newCloudAclEntrySet =
				new CloudAclEntrySet(AnonymousUserPrincipal.INSTANCE, DefaultCloudAclEntryConflictChecker.INSTANCE);
		CloudAclEntry<PublicPrivateCloudPermissionsPrincipal> privateAccessEntry =
				new CloudAclEntryBuilder<PublicPrivateCloudPermissionsPrincipal>(PublicPrivateCloudPermissionsPrincipal.class)
					.setPrincipal(new PublicPrivateCloudPermissionsPrincipal(BlobAccess.PRIVATE))
					.setType(AclEntryType.ALLOW)
					.build();
		newCloudAclEntrySet.addAclEntry(AnonymousUserPrincipal.INSTANCE, privateAccessEntry);
		Assert.assertEquals(1,  newCloudAclEntrySet.size());
		Assert.assertTrue(fileAttributeView.setAclFileAttributes(newCloudAclEntrySet).isEmpty());

		// Check by reading the ACL back again
		readAclFileAttributes = fileAttributeView.readAttributes();
		cloudAclEntrySet = readAclFileAttributes.getAclSet();
		assertPublicPrivateAccessAcl(cloudAclEntrySet, BlobAccess.PRIVATE);		
	}

	private void assertPublicPrivateAccessAcl(CloudAclEntrySet cloudAclEntrySet, BlobAccess blobAccess) {
		Assert.assertEquals(1, cloudAclEntrySet.size());
		CloudAclEntry<?> publicPrivatePerms = cloudAclEntrySet.iterator().next();
		Assert.assertEquals(PublicPrivateCloudPermissionsPrincipal.class, publicPrivatePerms.getPrincipalClass());
		Assert.assertEquals(AclEntryType.ALLOW, publicPrivatePerms.getType());
		Assert.assertEquals(blobAccess,
				((PublicPrivateCloudPermissionsPrincipal)publicPrivatePerms.getPrincipal()).getBlobAccess());
	}



	public void assertNotExists(String path) throws IOException {
		assertNotExists(new CloudPath(fileSystem, true, CONTAINER_NAME + CloudPath.DEFAULT_PATH_SEPARATOR + path));
	}

	public void assertNotExists(CloudPath cloudPath) throws IOException {
		try {
			impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, cloudPath);
			Assert.fail("File exists: " + cloudPath.getAllPaths());
		} catch (FileNotFoundException e) {
			// OK
		}
	}
	
	public void assertFileExists(CloudPath cloudPath) throws IOException {
		Assert.assertTrue(impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, cloudPath).isRegularFile());
	}
	
	public void assertFileExists(String filePath) throws IOException {
		assertFileExists(new CloudPath(fileSystem, true, CONTAINER_NAME + CloudPath.DEFAULT_PATH_SEPARATOR + filePath));
	}
	
	public void assertDirectoryExists(CloudPath cloudPath) throws IOException {
		Assert.assertTrue(impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, cloudPath).isDirectory());
	}

	public void assertDirectoryExists(String dirPath) throws IOException {
		assertDirectoryExists(new CloudPath(fileSystem, true, CONTAINER_NAME + CloudPath.DEFAULT_PATH_SEPARATOR + dirPath));
	}

}
