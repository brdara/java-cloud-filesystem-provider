package com.uk.xarixa.cloud.filesystem.core.nio.file;

public interface WatchKeyReadyListener {

	/**
	 * Queues up the watch key
	 * @param watchKey
	 * @return	true if the watch key could be queued (and {@link PollingJobWatchKey#lastQueueAttemptSucceeded()} is invoked), 
	 * 			false otherwise (and {@link PollingJobWatchKey#lastQueueAttemptFailed()} is invoked).
	 */
	boolean watchKeyReady(PollingJobWatchKey watchKey);

}
