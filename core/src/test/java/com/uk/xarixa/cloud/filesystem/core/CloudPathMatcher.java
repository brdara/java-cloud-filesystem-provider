package com.uk.xarixa.cloud.filesystem.core;

import java.nio.file.Path;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

public class CloudPathMatcher extends BaseMatcher<Path> {
	private CloudPath thisPath;

	public CloudPathMatcher(CloudPath thisPath) {
		this.thisPath = thisPath;
	}

	@Override
	public boolean matches(Object item) {
		if (!(item instanceof CloudPath)) {
			return false;
		}
		
		CloudPath other = (CloudPath)item;
		List<String> myAllPaths = thisPath.getAllPaths();
		List<String> otherAllPaths = ((CloudPath)other).getAllPaths();

		// Compare each path element
		boolean compareLeastMyPaths = myAllPaths.size() < otherAllPaths.size();
		for (int i=0; i<(compareLeastMyPaths ? myAllPaths.size() : otherAllPaths.size()); i++) {
			int compareResult = myAllPaths.get(i).compareTo(otherAllPaths.get(i));
			if (compareResult != 0) {
				return false;
			}
		}

		return myAllPaths.size() == otherAllPaths.size() ? true : false;
	}

	@Override
	public void describeTo(Description description) {
		description.appendText(StringUtils.join(thisPath.getAllPaths(), "/"));
	}
}
