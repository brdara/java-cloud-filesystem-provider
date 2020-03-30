package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * Contains all of the events 
 * @author brdar
 *
 */
public class PollingJobWatchKey implements WatchKey {
	private final AtomicBoolean valid = new AtomicBoolean(true);
	private AtomicBoolean lastQueueAttemptFailed = new AtomicBoolean(false);
	private CloudPath path;
	private Set<FileTreeComparisonWatchEvent> events = new HashSet<>();
	private ReentrantLock eventsSetLock = new ReentrantLock();

	class FileTreeComparisonWatchEvent implements WatchEvent<Object> {
		private final FileTreeComparisonEvent event;
		private Kind<Object> kind;

		FileTreeComparisonWatchEvent(FileTreeComparisonEvent event) {
			this.event = event;
			this.kind = (Kind<Object>)FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(event);
		}

		@Override
		public Kind<Object> kind() {
			return kind;
		}

		@Override
		public int count() {
			return 1;
		}

		@Override
		public Object context() {
			return event;
		}

		@Override
		public boolean equals(Object obj) {
			return event.equals(obj);
		}
		
		@Override
		public int hashCode() {
			return event.hashCode();
		}
	}

	public PollingJobWatchKey(CloudPath path) {
		this.path = path;
	}

	@Override
	public boolean isValid() {
		return valid.get();
	}
	
	public void addEvent(FileTreeComparisonEvent event) {
		eventsSetLock.lock();
		
		try {
			this.events.add(new FileTreeComparisonWatchEvent(event));
		} finally {
			eventsSetLock.unlock();
		}
	}

	public void addEvents(Collection<FileTreeComparisonEvent> events) {
		events.forEach(event -> addEvent(event));
	}

	/**
	 * Passes back a list of the current watch events. The {@link WatchEvent#context()} will return a 
	 * {@link FileTreeComparisonEvent}.
	 */
	@Override
	public List<WatchEvent<?>> pollEvents() {
		List<WatchEvent<?>> pollEvents;
		eventsSetLock.lock();
		
		try {
			pollEvents = new ArrayList<>(events);
			events.clear();
		} finally {
			eventsSetLock.unlock();
		}

		return pollEvents;
	}

	@Override
	public boolean reset() {
		// No action
		return valid.get();
	}

	@Override
	public void cancel() {
		valid.set(false);
		events.clear();
	}

	@Override
	public Watchable watchable() {
		return path;
	}
	
	/**
	 * Denotes that the last call to {@link WatchKeyReadyListener#watchKeyReady(PollingJobWatchKey) failed}
	 */
	public void lastQueueAttemptFailed() {
		lastQueueAttemptFailed.set(true);
	}

	/**
	 * Denotes that the last call to {@link WatchKeyReadyListener#watchKeyReady(PollingJobWatchKey) succeeded}
	 */
	public void lastQueueAttemptSucceeded() {
		lastQueueAttemptFailed.set(false);
	}

	/**
	 * Gets the state of the last call to {@link WatchKeyReadyListener#watchKeyReady(PollingJobWatchKey)}
	 * @return
	 */
	public boolean getLastQueueAttemptFailed() {
		return lastQueueAttemptFailed.get();
	}

	/**
	 * Whether there are any queued events available, either because {@link WatchKeyReadyListener#watchKeyReady(PollingJobWatchKey)}
	 * failed or events exist
	 * @return
	 */
	public boolean hasEvents() {
		return lastQueueAttemptFailed.get() || !events.isEmpty();	// Not bothered about locking the set here hah! The accessors are synchronized.
	}
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
}