package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.util.concurrent.locks.ReentrantLock;

public class PollingCloudWatchServiceFactory implements CloudWatchServiceFactory {
	private final ReentrantLock lock = new ReentrantLock();
	private PollingCloudWatchService cloudWatchService;

	@Override
	public CloudWatchService createWatchService(CloudWatchServiceConfiguration configuration) {
		if (cloudWatchService == null) {
			lock.lock();
			
			try {
				if (cloudWatchService == null) {
					cloudWatchService = new PollingCloudWatchService(configuration.getPollTimeMs());
				}
			} finally {
				lock.unlock();
			}
		}

		return cloudWatchService;
	}

}
