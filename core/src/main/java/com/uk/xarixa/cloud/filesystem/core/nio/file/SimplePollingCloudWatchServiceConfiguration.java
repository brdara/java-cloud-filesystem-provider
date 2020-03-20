package com.uk.xarixa.cloud.filesystem.core.nio.file;

public class SimplePollingCloudWatchServiceConfiguration implements CloudWatchServiceConfiguration {
	private final long pollTimeMs;

	public SimplePollingCloudWatchServiceConfiguration(long pollTimeMs) {
		this.pollTimeMs = pollTimeMs;
	}

	public long getPollTimeMs() {
		return pollTimeMs;
	}

	@Override
	public String localSyncDirectory() {
		return System.getProperty("user.home");
	}

}
