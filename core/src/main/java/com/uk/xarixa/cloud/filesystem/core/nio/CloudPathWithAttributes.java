package com.uk.xarixa.cloud.filesystem.core.nio;

import java.util.List;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;

/**
 * A {@link CloudPath} which has originated from a file listing
 */
public class CloudPathWithAttributes extends CloudPath {
	private final CloudBasicFileAttributes attributes;

	public CloudPathWithAttributes(CloudFileSystem fileSystem, boolean isAbsolute, CloudPath rootPath,
			String fullPath, CloudBasicFileAttributes attributes) {
		super(fileSystem, isAbsolute, rootPath, fullPath);
		this.attributes = attributes;
	}

	public CloudPathWithAttributes(CloudFileSystem fileSystem, boolean isAbsolute, List<String> rootPath,
			List<String> fullPath, CloudBasicFileAttributes attributes) {
		super(fileSystem, isAbsolute, rootPath, fullPath);
		this.attributes = attributes;
	}

	public CloudPathWithAttributes(CloudFileSystem fileSystem, boolean isAbsolute, List<String> fullPath,
			CloudBasicFileAttributes attributes) {
		super(fileSystem, isAbsolute, fullPath);
		this.attributes = attributes;
	}

	public CloudPathWithAttributes(CloudFileSystem fileSystem, boolean isAbsolute, String rootPath, String fullPath,
			CloudBasicFileAttributes attributes) {
		super(fileSystem, isAbsolute, rootPath, fullPath);
		this.attributes = attributes;
	}

	public CloudPathWithAttributes(CloudFileSystem fileSystem, boolean isAbsolute, String fullPath, CloudBasicFileAttributes attributes) {
		super(fileSystem, isAbsolute, fullPath);
		this.attributes = attributes;
	}

	public CloudPathWithAttributes(CloudPath cloudPath, boolean isAbsolute, List<String> rootPath,
			List<String> partialPath, CloudBasicFileAttributes attributes) {
		super(cloudPath, isAbsolute, rootPath, partialPath);
		this.attributes = attributes;
	}

	public CloudPathWithAttributes(CloudPath cloudPath, String fullPath, CloudBasicFileAttributes attributes) {
		super(cloudPath, fullPath);
		this.attributes = attributes;
	}

	public CloudBasicFileAttributes getAttributes() {
		return attributes;
	}

}
