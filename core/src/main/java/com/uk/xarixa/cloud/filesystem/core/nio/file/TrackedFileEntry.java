package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.Serializable;
import java.nio.file.Files;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

public class TrackedFileEntry implements Comparable<TrackedFileEntry>, Serializable {
	private static final long serialVersionUID = 1L;
	private CloudPath path;
	private boolean isFolder;
	
	public TrackedFileEntry(CloudPath path) {
		this(path, Files.isDirectory(path));
	}

	public TrackedFileEntry(CloudPath path, boolean isFolder) {
		this.path = path;
		this.isFolder = isFolder;
	}

	public CloudPath getPath() {
		return path;
	}
	public void setPath(CloudPath path) {
		this.path = path;
	}
	public boolean isFolder() {
		return isFolder;
	}
	public void setFolder(boolean isFolder) {
		this.isFolder = isFolder;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (!(obj instanceof TrackedFileEntry)) {
			return false;
		}
		
		return compareTo((TrackedFileEntry)obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(path)
				.append(isFolder)
				.toHashCode();
	}

	@Override
	public int compareTo(TrackedFileEntry other) {
		if (isFolder && !other.isFolder) {
			return 1;
		}

		if (!isFolder && !other.isFolder) {
			return -1;
		}

		return path.compareTo(other.path);
	}

}
