package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.WatchEvent.Modifier;

public enum CloudWatchModifier implements Modifier {

	/**
	 * A watch modifier to look at all of the paths in the subtree
	 */
	SUBTREE;

}
