package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

public interface CloudWatchService extends WatchService {

	/**
	 * Registers this path for watching
	 * @param path
	 * @param events
	 * @param modifiers
	 * @return The watch key
	 */
	WatchKey register(CloudPath path, Kind<?>[] events, Modifier... modifiers);

}
