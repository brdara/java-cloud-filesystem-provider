package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.WatchService;

/**
 * A factory for creating a {@link WatchService}
 */
public interface CloudWatchServiceFactory {

	CloudWatchService createWatchService(CloudWatchServiceConfiguration configuration);

}
