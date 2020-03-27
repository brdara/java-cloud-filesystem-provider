package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class TrackedFileEntry implements Comparable<TrackedFileEntry>, Serializable {
	private static final long serialVersionUID = 1L;
	private Path path;
	private boolean isFolder;
	private String checkSum;
	
	public TrackedFileEntry(Path path) {
		this(path, Files.isDirectory(path));
	}

	public TrackedFileEntry(Path path, String checksum) {
		this(path, Files.isDirectory(path), checksum);
	}

	public TrackedFileEntry(Path path, boolean isFolder) {
		this(path, isFolder, null);
	}

	public TrackedFileEntry(Path path, boolean isFolder, String checksum) {
		this.path = path;
		this.isFolder = isFolder;
		this.checkSum = checksum;
	}

	public Path getPath() {
		return path;
	}
	public void setPath(Path path) {
		this.path = path;
	}
	public boolean isFolder() {
		return isFolder;
	}
	public void setFolder(boolean isFolder) {
		this.isFolder = isFolder;
	}
	
	public String getCheckSum() {
		return checkSum;
	}

	public void setCheckSum(String checkSum) {
		this.checkSum = checkSum;
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
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
