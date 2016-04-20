package com.uk.xarixa.cloud.filesystem.core.nio.file;

public class DefaultCloudWatchServiceConfiguration implements CloudWatchServiceConfiguration {
	private final long pollTimeMs;

	public DefaultCloudWatchServiceConfiguration(long pollTimeMs) {
		this.pollTimeMs = pollTimeMs;
	}

	public long getPollTimeMs() {
		return pollTimeMs;
	}

}
