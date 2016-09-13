package com.uk.xarixa.cloud.filesystem.core.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.StringUtils;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * A simple path iterator
 */
public class PathIterator implements Iterator<String> {
	private final String path;
	private int startIndex;

	public PathIterator(String path) {
		if (StringUtils.isBlank(path) || StringUtils.equals(path, CloudPath.DEFAULT_PATH_SEPARATOR)) {
			throw new IllegalArgumentException("Cannot iterate over an empty path: '" + path + "'");
		}

		this.path = path;
		this.startIndex = path.startsWith(CloudPath.DEFAULT_PATH_SEPARATOR) ? 1 : 0;
	}

	@Override
	public boolean hasNext() {
		return startIndex != -1;
	}

	@Override
	public String next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		int foundSeparator = StringUtils.indexOf(path, CloudPath.DEFAULT_PATH_SEPARATOR_CHAR, startIndex);
		String pathPart;
		
		if (foundSeparator == -1) {
			// Return the rest of the path
			pathPart = StringUtils.substring(path, startIndex);
			startIndex = -1;
		} else {
			pathPart = StringUtils.substring(path, startIndex, foundSeparator);
			startIndex = foundSeparator + 1;
		}

		return pathPart;
	}


}
