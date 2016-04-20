package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;

/**
 * A simple implementation for {@link FileStore}
 */
public class CloudFileStore extends FileStore {
	public static final String CLOUD_FILE_STORE_TYPE = "cloud";
	private final CloudHostConfiguration settings;

	public CloudFileStore(CloudHostConfiguration settings) {
		this.settings = settings;
	}

	/**
	 * Returns {@link CloudHostConfiguration#getName()}
	 */
	@Override
	public String name() {
		return settings.getName();
	}

	@Override
	public String type() {
		return CLOUD_FILE_STORE_TYPE;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public long getTotalSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUsableSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUnallocatedSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
		return false;
	}

	@Override
	public boolean supportsFileAttributeView(String name) {
		return false;
	}

	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
		throw new IllegalArgumentException("No file store attribute view available");
	}

	@Override
	public Object getAttribute(String attribute) throws IOException {
		throw new IllegalArgumentException("No file store attribute view available");
	}

}
