package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jmock.Expectations;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.scalified.tree.TreeNode;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonResult;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonSide;

@RunWith(BlockJUnit4ClassRunner.class)
public class PollingWatchServiceJobTest extends AbstractFileTestsHelper {

	private PollingWatchServiceJob serviceJob;

	@Override
	void preSetUp() {
	}

	@Override
	void postSetUp() {
		serviceJob = new PollingWatchServiceJob();
	}

	@Test
	public void testExecuteAddsNoEventsWhenTreeHierarchyIsEquivalent() throws IOException, JobExecutionException {
		JobExecutionContext jobContext = context.mock(JobExecutionContext.class);
		Set<Kind<?>> kinds = Collections.emptySet();
		Set<Modifier> modifiers = Collections.emptySet();
		CloudPath rootPath = createCloudPath("/root", true);
		PollingJobWatchKey watchKey = new PollingJobWatchKey(rootPath);
		WatchKeyReadyListener watchKeyReadyListener = context.mock(WatchKeyReadyListener.class);
		JobDataMap jobDataMap = new JobDataMap(Map.of(
				PollingWatchServiceJob.JOB_KIND_KEY, kinds,
				PollingWatchServiceJob.JOB_MODIFIERS_KEY, modifiers,
				PollingWatchServiceJob.JOB_WATCH_KEY, watchKey,
				PollingWatchServiceJob.JOB_WATCH_KEY_READY_LISTENER, watchKeyReadyListener
		));

		CloudPath file1 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2 = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content"));
		createDirectoryListing(rootPath, file1, file2);

		context.checking(new Expectations() {{
			allowing(jobContext).getMergedJobDataMap(); will(returnValue(jobDataMap));
		}});

		// First pass saves the state
		serviceJob.execute(jobContext);
		
		TreeNode<TrackedFileEntry> previousState = getPreviousState(jobContext);
		Assert.assertNotNull(previousState);

		// Set up a new listing with the same content
		CloudPath rootPath_Run2 = createCloudPath("/root", true);
		CloudPath file1_Run2 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2_Run2 = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content"));
		createDirectoryListing(rootPath_Run2, file1_Run2, file2_Run2);
		serviceJob.execute(jobContext);
		Assert.assertEquals(previousState, getPreviousState(jobContext));
	}

	private TreeNode<TrackedFileEntry> getPreviousState(JobExecutionContext jobContext) {
		TreeNode<TrackedFileEntry> previousState = (TreeNode<TrackedFileEntry>)jobContext.getMergedJobDataMap().get(PollingWatchServiceJob.JOB_PREVIOUS_STATE);
		return previousState;
	}

	@Test
	public void testExecuteAddsAnEventsWhenFileContentDiffers() throws IOException, JobExecutionException {
		JobExecutionContext jobContext = context.mock(JobExecutionContext.class);
		Set<Kind<?>> kinds = Collections.emptySet();
		Set<Modifier> modifiers = Collections.emptySet();
		CloudPath rootPath = createCloudPath("/root", true);
		PollingJobWatchKey watchKey = new PollingJobWatchKey(rootPath);
		WatchKeyReadyListener watchKeyReadyListener = context.mock(WatchKeyReadyListener.class);
		JobDataMap jobDataMap = new JobDataMap(Map.of(
				PollingWatchServiceJob.JOB_KIND_KEY, kinds,
				PollingWatchServiceJob.JOB_MODIFIERS_KEY, modifiers,
				PollingWatchServiceJob.JOB_WATCH_KEY, watchKey,
				PollingWatchServiceJob.JOB_WATCH_KEY_READY_LISTENER, watchKeyReadyListener
		));

		// Set up the job
		context.checking(new Expectations() {{
			allowing(jobContext).getMergedJobDataMap(); will(returnValue(jobDataMap));
		}});

		// Set up the paths
		CloudPath file1 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2 = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content"));
		createDirectoryListing(rootPath, file1, file2);

		// First pass saves the state
		serviceJob.execute(jobContext);
		
		TreeNode<TrackedFileEntry> previousState = getPreviousState(jobContext);
		Assert.assertNotNull(previousState);

		// Set up a new listing with the differing
		CloudPath rootPath_Run2 = createCloudPath("/root", true);
		CloudPath file1_Run2 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2_Run2Modified = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content MODIFIED")); // Different content (Md5)
		createDirectoryListing(rootPath_Run2, file1_Run2, file2_Run2Modified);

		context.checking(new Expectations() {{
			exactly(1).of(watchKeyReadyListener).watchKeyReady(watchKey); will(returnValue(true));
		}});
		
		serviceJob.execute(jobContext);
		
		// The state changes because file 2 has different content
		Assert.assertNotEquals(previousState, getPreviousState(jobContext));

		// Check the events
		Assert.assertTrue(watchKey.hasEvents());
		Assert.assertFalse(watchKey.getLastQueueAttemptFailed());

		// Detect the events
		List<WatchEvent<?>> events = watchKey.pollEvents();
		Assert.assertEquals(1, events.size());
		WatchEvent<?> watchEvent = events.get(0);
		Assert.assertNull(watchEvent.kind());
		FileTreeComparisonEvent event = (FileTreeComparisonEvent)watchEvent.context();
		Assert.assertEquals(file2, event.getLeftPath());
		Assert.assertEquals(file2_Run2Modified, event.getRightPath());
		Assert.assertEquals(ComparisonResult.FILE_DIGEST_MISMATCH, event.getResult());
		Assert.assertEquals(ComparisonSide.BOTH, event.getSide());

		// Assert that there are no more events in the watch key now
		Assert.assertFalse(watchKey.hasEvents());
	}

	@Test
	public void testExecuteWillDeferAddingEventsAndChangingStateIfTheWatchKeyReadyListenerDisallowsIt() throws IOException, JobExecutionException {
		JobExecutionContext jobContext = context.mock(JobExecutionContext.class);
		Set<Kind<?>> kinds = Collections.emptySet();
		Set<Modifier> modifiers = Collections.emptySet();
		CloudPath rootPath = createCloudPath("/root", true);
		PollingJobWatchKey watchKey = new PollingJobWatchKey(rootPath);
		WatchKeyReadyListener watchKeyReadyListener = context.mock(WatchKeyReadyListener.class);
		JobDataMap jobDataMap = new JobDataMap(Map.of(
				PollingWatchServiceJob.JOB_KIND_KEY, kinds,
				PollingWatchServiceJob.JOB_MODIFIERS_KEY, modifiers,
				PollingWatchServiceJob.JOB_WATCH_KEY, watchKey,
				PollingWatchServiceJob.JOB_WATCH_KEY_READY_LISTENER, watchKeyReadyListener
		));

		// Set up the job
		context.checking(new Expectations() {{
			allowing(jobContext).getMergedJobDataMap(); will(returnValue(jobDataMap));
		}});

		// Set up the paths
		CloudPath file1 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2 = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content"));
		createDirectoryListing(rootPath, file1, file2);

		// First pass saves the state
		serviceJob.execute(jobContext);
		
		TreeNode<TrackedFileEntry> previousState = getPreviousState(jobContext);
		Assert.assertNotNull(previousState);

		// Set up a new listing with the differing
		CloudPath rootPath_Run2 = createCloudPath("/root", true);
		CloudPath file1_Run2 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2_Run2Modified = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content MODIFIED")); // Different content (Md5)
		createDirectoryListing(rootPath_Run2, file1_Run2, file2_Run2Modified);

		context.checking(new Expectations() {{
			exactly(1).of(watchKeyReadyListener).watchKeyReady(watchKey); will(returnValue(false));	// Fail to add the watch key
			watchKey.lastQueueAttemptFailed();	// This would be set up in the listener
		}});
		
		serviceJob.execute(jobContext);
		
		// The state should not change because the watch key could not be set to the ready state
		Assert.assertEquals(previousState, getPreviousState(jobContext));

		// Check the events
		Assert.assertTrue(watchKey.hasEvents());
		Assert.assertTrue(watchKey.getLastQueueAttemptFailed());

		// Detect the events in the watch key
		List<WatchEvent<?>> events = watchKey.pollEvents();
		Assert.assertEquals(1, events.size());
		WatchEvent<?> watchEvent = events.get(0);
		Assert.assertNull(watchEvent.kind());
		FileTreeComparisonEvent event = (FileTreeComparisonEvent)watchEvent.context();
		Assert.assertEquals(file2, event.getLeftPath());
		Assert.assertEquals(file2_Run2Modified, event.getRightPath());
		Assert.assertEquals(ComparisonResult.FILE_DIGEST_MISMATCH, event.getResult());
		Assert.assertEquals(ComparisonSide.BOTH, event.getSide());

		// We do it now a third time...
		CloudPath rootPath_Run3 = createCloudPath("/root", true);
		CloudPath file1_Run3 = createCloudPath("/root/file1.txt", false, createMd5Digest("File 1 content"));
		CloudPath file2_Run3Modified = createCloudPath("/root/file2.txt", false, createMd5Digest("File 2 content MODIFIED")); // Different content (Md5)
		createDirectoryListing(rootPath_Run3, file1_Run3, file2_Run3Modified);

		context.checking(new Expectations() {{
			exactly(1).of(watchKeyReadyListener).watchKeyReady(watchKey); will(returnValue(true));	// Now succeed on this run
			watchKey.lastQueueAttemptSucceeded();	// This would be set up in the listener
		}});
		
		serviceJob.execute(jobContext);

		// The state should not change because the watch key could not be set to the ready state
		Assert.assertNotEquals(previousState, getPreviousState(jobContext));

		// Check the events
		Assert.assertTrue(watchKey.hasEvents());
		Assert.assertFalse(watchKey.getLastQueueAttemptFailed());

		// Detect the events in the watch key
		List<WatchEvent<?>> events2 = watchKey.pollEvents();
		Assert.assertEquals(1, events2.size());
		WatchEvent<?> watchEvent2 = events2.get(0);
		Assert.assertEquals((FileTreeComparisonEvent)watchEvent.context(), (FileTreeComparisonEvent)watchEvent2.context());	// The event should be the same as previously
		
		// Assert that there are no more events in the watch key now
		Assert.assertFalse(watchKey.hasEvents());
	}

}
