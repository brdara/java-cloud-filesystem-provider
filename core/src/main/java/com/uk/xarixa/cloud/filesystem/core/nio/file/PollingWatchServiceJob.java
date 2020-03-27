package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.util.Set;
import java.util.SortedSet;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scalified.tree.TreeNode;

/**
 * <p>
 * Periodic job which runs through Quartz that checks the state of the file system for the given job path.
 * Jobs pass in the following keys in their job contexts when registered through the
 * {@link PollingCloudWatchService#register(com.uk.xarixa.cloud.filesystem.core.nio.CloudPath, Kind[], Modifier...)} method:
 * <ul>
 * <li>{@link PollingWatchServiceJob#JOB_KIND_KEY} - A {@link java.util.Set} of {@link Kind}</li>
 * <li>{@link PollingWatchServiceJob#JOB_MODIFIERS_KEY} - A {@link java.util.Set} of {@link Modifier}</li>
 * <li>{@link PollingWatchServiceJob#JOB_WATCH_KEY} - The {@link PollingJobWatchKey} with the path to process</li>
 * <li>{@link PollingWatchServiceJob#JOB_WATCH_KEY_READY_LISTENER} - The {@link WatchKeyReadyListener} to notify that events for the watch key are ready</li>
 * </ul>
 * </p>
 * <p>
 * With this implementation, events should not be lost. If they cannot be added then an exception will occur or if the
 * {@link WatchKeyReadyListener#watchKeyReady(PollingJobWatchKey)} failed then the watch key will remain and the previous
 * state will remain so that the next time this is invoked it will go through the same process, and if room becomes available
 * it will succeed next time.
 * </p>
 * 
 * @author brdar
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class PollingWatchServiceJob implements Job {
	public static final String JOB_KIND_KEY = "kind";
	public static final String JOB_MODIFIERS_KEY = "modifiers";
	public static final String JOB_WATCH_KEY = "watchKey";
	public static final String JOB_WATCH_KEY_READY_LISTENER = "watchKeyReadyListener";
	public static final String JOB_PREVIOUS_STATE = "previousState";
	private static final Logger LOG = LoggerFactory.getLogger(PollingWatchServiceJob.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		final JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		final Set<Kind<?>> kinds = (Set<Kind<?>>)jobDataMap.get(JOB_KIND_KEY);
		final Set<Modifier> modifiers = (Set<Modifier>)jobDataMap.get(JOB_MODIFIERS_KEY);
		final PollingJobWatchKey watchKey = (PollingJobWatchKey)jobDataMap.get(JOB_WATCH_KEY);
		final WatchKeyReadyListener watchKeyReadyListener = (WatchKeyReadyListener)jobDataMap.get(JOB_WATCH_KEY_READY_LISTENER);
		final Path path = (Path)watchKey.watchable();
		TreeNode<TrackedFileEntry> previousState = (TreeNode<TrackedFileEntry>)jobDataMap.get(JOB_PREVIOUS_STATE);

		if (previousState == null) {
			// Take the initial state
			LOG.debug("Getting initial state for path {}...", path);
			previousState = FileHierarchyHelper.getTrackedFileEntryTree(path);
			updatePreviousState(jobDataMap, previousState);
			LOG.debug("Finished getting initial state for path {}...", path);
		} else {
			// Take the new state and compare
			LOG.debug("Getting new state for path {}...", path);
			TreeNode<TrackedFileEntry> currentState = FileHierarchyHelper.getTrackedFileEntryTree(path);
			
			try {
				if (compareFileHierarchy(watchKey, kinds, modifiers, watchKeyReadyListener, currentState, previousState)) {
					// Only store the new state if the watch key could be added to the queue and all events were
					// processed
					LOG.debug("Storing current state for {}", watchKey);
					updatePreviousState(jobDataMap, currentState);
				}
			} catch (Exception e) {
				LOG.error("Could not compare file hierarchy for watch key: {}", watchKey, e);
			}

			LOG.debug("Finished getting new state for path {}...", path);
		}
	}

	private void updatePreviousState(JobDataMap jobDataMap, TreeNode<TrackedFileEntry> newState) {
		jobDataMap.put(JOB_PREVIOUS_STATE, newState);
	}

	private boolean compareFileHierarchy(PollingJobWatchKey watchKey, Set<Kind<?>> kinds, Set<Modifier> modifiers, 
			WatchKeyReadyListener watchKeyReadyListener, TreeNode<TrackedFileEntry> currentState, TreeNode<TrackedFileEntry> previousState) {
		CollatingByPathFileTreeComparisonEventHandler handler = new CollatingByPathFileTreeComparisonEventHandler(kinds);
		FileHierarchyHelper.compareFileTreeHierarchies(handler, previousState, currentState);
		SortedSet<FileTreeComparisonEvent> events = handler.getEvents();
		
		if (!events.isEmpty()) {
			watchKey.addEvents(events);
		}

		if (watchKey.hasEvents()) {
			return watchKeyReadyListener.watchKeyReady(watchKey);
		}
		
		return false;
	}

}