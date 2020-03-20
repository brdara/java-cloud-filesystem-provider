package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.Watchable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PollingJobWatchKey implements WatchKey {
	private final AtomicBoolean valid = new AtomicBoolean(true);

	@Override
	public boolean isValid() {
		return valid.get();
	}

	@Override
	public List<WatchEvent<?>> pollEvents() {
		return null;
	}

	@Override
	public boolean reset() {
		// No action
		return valid.get();
	}

	@Override
	public void cancel() {
		valid.set(false);
	}

	@Override
	public Watchable watchable() {
		return null;
	}
	
}