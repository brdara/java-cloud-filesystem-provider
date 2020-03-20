package com.uk.xarixa.cloud.filesystem.core.nio.file;

/**
 * Configuration for the {@link CloudWatchServiceFactory}
 */
public interface CloudWatchServiceConfiguration {

	/**
	 * This is the poll time that the watcher should use to refetch requests from the cloud in ms.
	 * Note that this may cause (depending upon the implementation) a new cloud listing request
	 * for the specified path(s) in the watcher every single time, resulting in high data I/O.
	 * @return
	 */
	long getPollTimeMs();
	
	/**
	 * Points to a directory which is used to store the sync state
	 * @return
	 */
	String localSyncDirectory();

}
