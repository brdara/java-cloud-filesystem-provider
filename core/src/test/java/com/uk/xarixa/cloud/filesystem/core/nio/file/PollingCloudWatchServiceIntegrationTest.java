package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jclouds.blobstore.domain.BlobAccess;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.MediaType;
import com.uk.xarixa.cloud.filesystem.core.AbstractJCloudsIntegrationTest;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.CloudPermissionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentTypeFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.DefaultCloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonResult;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonSide;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

@RunWith(BlockJUnit4ClassRunner.class)
public class PollingCloudWatchServiceIntegrationTest extends AbstractJCloudsIntegrationTest {
	private final static Logger LOG = LoggerFactory.getLogger(PollingCloudWatchServiceIntegrationTest.class);
	private static final long POLLING_TIME_MS = 10000;
	private PollingCloudWatchService pollingService;
	private DefaultCloudFileSystemImplementation cfs;
	
	@Override
	protected void postSetUp() {
		cfs = new DefaultCloudFileSystemImplementation();
		pollingService = new PollingCloudWatchService(POLLING_TIME_MS);
	}
	
	@Override
	protected void postTearDown() {
		try {
			pollingService.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void testPollingDiscoversFileChanges() throws IOException, InterruptedException {
		CloudPath root = new CloudPath(containerPath, "dir1");

		try {
			createDirectory(root);
			createRawContent("dir1/file1_1", "File 1_1".getBytes("UTF-8"));

			// Polling will get started now
			WatchKey originalWatchKey = pollingService.register(root, new Kind<?>[0]);

			// Poll for new events
			Assert.assertNull((PollingJobWatchKey)pollingService.poll((int)(POLLING_TIME_MS * 1.2), TimeUnit.MILLISECONDS));

			// Change the file
			CloudPath filePath = new CloudPath(root, "file1_1");
			try (SeekableByteChannel channel = Files.newByteChannel(filePath,
					EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE),
					new CloudPermissionFileAttribute<>(BlobAccess.PRIVATE),
					new ContentTypeFileAttribute(MediaType.PLAIN_TEXT_UTF_8))) {
				channel.write(ByteBuffer.wrap("File 1_1 Modified Content".getBytes("UTF-8")));
			}

			// Poll for the change
			PollingJobWatchKey change1 = (PollingJobWatchKey)pollingService.poll((int)(POLLING_TIME_MS * 1.2), TimeUnit.MILLISECONDS);
			Assert.assertNotNull(change1);
			Assert.assertTrue(change1.hasEvents());

			// Get the change and check it is as expected
			List<WatchEvent<?>> pollEvents = change1.pollEvents();
			Assert.assertEquals(1, pollEvents.size());
			WatchEvent<?> watchEvent = pollEvents.get(0);
			FileTreeComparisonEvent event = (FileTreeComparisonEvent)watchEvent.context();
			Assert.assertEquals(ComparisonSide.BOTH, event.getSide());
			Assert.assertEquals(ComparisonResult.FILE_DIGEST_MISMATCH, event.getResult());
			Assert.assertTrue("Invalid path: " + event.getRightPath(), event.getRightPath().endsWith("dir1/file1_1"));

			Assert.assertFalse(change1.hasEvents());

			// Now after we have processed that event we should not get it again
			PollingJobWatchKey change2 = (PollingJobWatchKey)pollingService.poll((int)(POLLING_TIME_MS * 1.2), TimeUnit.MILLISECONDS);
			Assert.assertNull(change2);

		} finally {
			try {
				((CloudFileSystemProvider)provider).delete(root, DeleteOption.RECURSIVE);
			} catch (Exception e) {
				LOG.error("Cannot delete root", e);
			}
		}
	}

	void createDirectory(CloudPath dir) throws IOException {
		cfs.createDirectory(blobStoreContext, dir);
	}

}
