package com.uk.xarixa.cloud.filesystem.core.nio.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.jclouds.io.Payload;
import org.jmock.Expectations;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.AbstractJCloudsIntegrationTest;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudFileChannelIntegrationTest extends AbstractJCloudsIntegrationTest {

	@Test
	public void testCreateCloudFileChannelForCreatNewFailsIfTheFileExists() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		try {
			CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath, EnumSet.of(StandardOpenOption.CREATE_NEW));
			channel.close();
			Assert.fail("Opening a channel to an existing file with the create new option should fail");
		} catch (FileAlreadyExistsException e) {
			// OK
		}		
	}

	@Test
	public void testCreateCloudFileChannelForCreatNewWillCreateANewFileAndWriteContentToIt() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(false));
		}});

		String content = "This is my sentence";
		CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath,
				EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
		try {
			Assert.assertEquals(0L, channel.position());

			// Append to the file
			channel.write(ByteBuffer.wrap(content.getBytes("UTF-8")));
		} finally {
			// Sync to S3
			channel.close();
		}
		
		// Now check the file content
		List<String> contentAsLines = getContentAsLines(pathName);
		Assert.assertNotNull(contentAsLines);
		Assert.assertEquals(1, contentAsLines.size());
		Assert.assertEquals(content, contentAsLines.get(0));
	}

	@Test
	public void testCreateCloudFileChannelForCreateOnlyOverwritesAnExistingFile() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		String newContent = "This is my new sentence";
		CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath,
				EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE));
		try {
			Assert.assertEquals(0L, channel.position());

			// Append to the file
			channel.write(ByteBuffer.wrap(newContent.getBytes("UTF-8")));
		} finally {
			// Sync to S3
			channel.close();
		}
		
		// Now check the file content
		List<String> contentAsLines = getContentAsLines(pathName);
		Assert.assertNotNull(contentAsLines);
		Assert.assertEquals(1, contentAsLines.size());
		Assert.assertEquals(newContent, contentAsLines.get(0));
	}

	@Test
	public void testCreateCloudFileChannelForWriteOnlyOverwritesAnExistingFile() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		String newContent = "This is my new sentence";
		CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath, EnumSet.of(StandardOpenOption.WRITE));
		try {
			Assert.assertEquals(0L, channel.position());

			// Append to the file
			channel.write(ByteBuffer.wrap(newContent.getBytes("UTF-8")));
		} finally {
			// Sync to S3
			channel.close();
		}
		
		// Now check the file content
		List<String> contentAsLines = getContentAsLines(pathName);
		Assert.assertNotNull(contentAsLines);
		Assert.assertEquals(1, contentAsLines.size());
		Assert.assertEquals(newContent, contentAsLines.get(0));
	}

	@Test
	public void testCreateCloudFileChannelForAppendWillAddTextToTheEndOfTheFile() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		String appendedContent = ". And now another sentence.";
		CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath, EnumSet.of(StandardOpenOption.APPEND));
		try {
			Assert.assertNotEquals(0L, channel.position());

			// Append to the file
			channel.write(ByteBuffer.wrap(appendedContent.getBytes("UTF-8")));
		} finally {
			// Sync to S3
			channel.close();
		}
		
		// Now check the file content
		List<String> contentAsLines = getContentAsLines(pathName);
		Assert.assertNotNull(contentAsLines);
		Assert.assertEquals(1, contentAsLines.size());
		Assert.assertEquals(originalContent + appendedContent, contentAsLines.get(0));
	}

	@Test
	public void testCreateCloudFileChannelForceMethodWillWriteContentToCloudStore() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		String appendedContent = ". And now another sentence.";
		String appendedContent2 = "\nA second sentence on another line.";
		String appendedContent3 = "\nA third sentence on another line.";
		AuditingCloudFileChannelInterceptor auditingCloudFileChannelInterceptor = new AuditingCloudFileChannelInterceptor();
		CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath,
				auditingCloudFileChannelInterceptor, EnumSet.of(StandardOpenOption.APPEND));
		try {
			Assert.assertNotEquals(0L, channel.position());

			// Append to the file
			channel.write(ByteBuffer.wrap(appendedContent.getBytes("UTF-8")));
			channel.force(false);
			auditingCloudFileChannelInterceptor.assertFullAuditRan();
			List<String> content = getContentAsLines(pathName);
			Assert.assertEquals(originalContent + appendedContent, content.get(0));

			// Append to the file again
			channel.write(ByteBuffer.wrap(appendedContent2.getBytes("UTF-8")));
			channel.force(false);
			auditingCloudFileChannelInterceptor.assertFullAuditRan();
			content = getContentAsLines(pathName);
			Assert.assertEquals(2, content.size());
			Assert.assertEquals(originalContent + appendedContent, content.get(0));
			Assert.assertEquals(StringUtils.remove(appendedContent2, "\n"), content.get(1));

			// Now the final write which tests that close still flushes the content
			channel.write(ByteBuffer.wrap(appendedContent3.getBytes("UTF-8")));
		} finally {
			// Sync to S3
			channel.close();
			auditingCloudFileChannelInterceptor.assertFullAuditRan();
		}
		
		// Now check the file content
		List<String> contentAsLines = getContentAsLines(pathName);
		Assert.assertNotNull(contentAsLines);
		Assert.assertEquals(3, contentAsLines.size());
		Assert.assertEquals(originalContent + appendedContent, contentAsLines.get(0));
		Assert.assertEquals(StringUtils.remove(appendedContent2, "\n"), contentAsLines.get(1));
		Assert.assertEquals(StringUtils.remove(appendedContent3, "\n"), contentAsLines.get(2));
	}
	
	@Test
	public void testCreateCloudFileChannelWithSyncOptionWillWriteUpdatedContentToCloudStore() throws IOException {
		String pathName = "content/cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		String appendedContent = ". And now another sentence.";
		String appendedContent2 = "\nA second sentence on another line.";
		String appendedContent3 = "\nA third sentence on another line.";
		AuditingCloudFileChannelInterceptor auditingCloudFileChannelInterceptor = new AuditingCloudFileChannelInterceptor();
		CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath, auditingCloudFileChannelInterceptor,
				EnumSet.of(StandardOpenOption.SYNC, StandardOpenOption.APPEND));
		try {
			Assert.assertNotEquals(0L, channel.position());

			// Append to the file
			channel.write(ByteBuffer.wrap(appendedContent.getBytes("UTF-8")));
			auditingCloudFileChannelInterceptor.assertFullAuditRan();
			List<String> content = getContentAsLines(pathName);
			Assert.assertEquals(originalContent + appendedContent, content.get(0));

			// Append to the file again
			channel.write(ByteBuffer.wrap(appendedContent2.getBytes("UTF-8")));
			auditingCloudFileChannelInterceptor.assertFullAuditRan();
			content = getContentAsLines(pathName);
			Assert.assertEquals(2, content.size());
			Assert.assertEquals(originalContent + appendedContent, content.get(0));
			Assert.assertEquals(StringUtils.remove(appendedContent2, "\n"), content.get(1));

			// Now the final write which tests that close still flushes the content
			channel.write(ByteBuffer.wrap(appendedContent3.getBytes("UTF-8")));
		} finally {
			// Sync to S3
			channel.close();
			auditingCloudFileChannelInterceptor.assertFullAuditRan();
		}
		
		// Now check the file content
		List<String> contentAsLines = getContentAsLines(pathName);
		Assert.assertNotNull(contentAsLines);
		Assert.assertEquals(3, contentAsLines.size());
		Assert.assertEquals(originalContent + appendedContent, contentAsLines.get(0));
		Assert.assertEquals(StringUtils.remove(appendedContent2, "\n"), contentAsLines.get(1));
		Assert.assertEquals(StringUtils.remove(appendedContent3, "\n"), contentAsLines.get(2));
	}
	
	@Test
	public void testFileChannelReadWillCopyToTheLocalFileSystemForReadingOfTheFile() throws IOException {
		String pathName = "cloud-file-channel-test.txt";
		String originalContent = "This is my content";
		createRawContent(pathName, originalContent.getBytes("UTF-8"));
		CloudPath cloudPath = context.mock(CloudPath.class);
		
		context.checking(new Expectations() {{
			allowing(cloudPath).getContainerName();
			will(returnValue(CONTAINER_NAME));
			
			allowing(cloudPath).getPathName();
			will(returnValue(pathName));

			allowing(cloudPath).exists();
			will(returnValue(true));
		}});

		// Open file for reading
		try (CloudFileChannel channel = new CloudFileChannel(blobStoreContext, cloudPath, EnumSet.of(StandardOpenOption.READ))) {
			ByteBuffer buff = ByteBuffer.allocate((int)channel.size());
			channel.read(buff);
		}
	}
	
	static class AuditingCloudFileChannelInterceptor extends DefaultCloudFileChannelTransport {
		boolean lastPreSyncRan = false;
		boolean lastPostSyncRan = false;
		boolean createPayloadInvoked = false;

		@Override
		public void preSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetadata) {
			super.preSyncToCloud(cloudFileChannel, writeMetadata);
			lastPreSyncRan = true;
		}

		@Override
		public Payload createPayload(Path localFile) {
			Payload payload = super.createPayload(localFile);
			createPayloadInvoked = true;
			return payload;
		}

		@Override
		public void postSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetadata) {
			super.postSyncToCloud(cloudFileChannel, writeMetadata);
			lastPostSyncRan = true;
		}
		
		void resetAudit() {
			lastPreSyncRan = false;
			lastPostSyncRan = false;
			createPayloadInvoked = false;
		}
		
		void assertFullAuditRan() {
			Assert.assertTrue(lastPreSyncRan);
			Assert.assertTrue(lastPostSyncRan);
			Assert.assertTrue(createPayloadInvoked);
			resetAudit();
		}
	}

}
