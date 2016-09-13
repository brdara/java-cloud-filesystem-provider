package com.uk.xarixa.cloud.filesystem.core.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.StringUtils;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * A simple reverse path iterator
 */
public class ReversePathIterator implements Iterator<String> {
	private final String path;
	private int startIndex;

	public ReversePathIterator(String path) {
		if (StringUtils.isBlank(path) || StringUtils.equals(path, CloudPath.DEFAULT_PATH_SEPARATOR)) {
			throw new IllegalArgumentException("Cannot iterate over an empty path: '" + path + "'");
		}

		this.path = path;
		this.startIndex = path.endsWith(CloudPath.DEFAULT_PATH_SEPARATOR) ? path.length() - 2 : path.length() - 1;
	}

	@Override
	public boolean hasNext() {
		return startIndex >= 0;
	}

	@Override
	public String next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		int foundSeparator = reverseIndexOf(path, CloudPath.DEFAULT_PATH_SEPARATOR_CHAR, startIndex);
		String pathPart;
		
		if (foundSeparator == -1) {
			// Return the rest of the path
			pathPart = StringUtils.substring(path, 0, startIndex + 1);
			startIndex = -1;
		} else {
			pathPart = StringUtils.substring(path, foundSeparator + 1, startIndex + 1);
			startIndex = foundSeparator - 1;
		}

		return pathPart;
	}
	
	private int reverseIndexOf(String str, char character, int fromIndex) {
		for (int i=fromIndex; i>=0; i--) {
			if (str.charAt(i) == character) {
				return i;
			}
		}

		return -1;
	}


}
