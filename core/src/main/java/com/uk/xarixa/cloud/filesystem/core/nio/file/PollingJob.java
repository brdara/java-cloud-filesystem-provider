package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;
import com.scalified.tree.TreeNode;
import com.scalified.tree.multinode.ArrayMultiTreeNode;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

public class PollingJob implements Job {
	public static final String JOB_PATH_KEY = "path";
	public static final String JOB_KIND_KEY = "kind";
	public static final String JOB_MODIFIERS_KEY = "modifiers";
	public static final String JOB_WATCH_KEY = "watchKey";
	private static final Logger LOG = LoggerFactory.getLogger(PollingJob.class);

	private AtomicBoolean isFirstRun = new AtomicBoolean(true);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		CloudPath path = (CloudPath)context.get(JOB_PATH_KEY);
		List<Kind<?>> kinds = (List<Kind<?>>)context.get(JOB_KIND_KEY);
		List<Modifier> modifiers = (List<Modifier>)context.get(JOB_MODIFIERS_KEY);
		PollingJobWatchKey watchKey = (PollingJobWatchKey)context.get(JOB_WATCH_KEY);
		
		if (isFirstRun.get()) {
			inspectCurrentState(path);
		} else {
			
		}
	}

	void inspectCurrentState(CloudPath path) {
		final TreeNode<TrackedFileEntry> root = new ArrayMultiTreeNode<>(new TrackedFileEntry(path));
		final AtomicReference<TreeNode<TrackedFileEntry>> lastProcessedNode = new AtomicReference(root);

		FileSystemProviderHelper.iterateOverDirectoryContents(path.getFileSystem(), Optional.ofNullable(path),
				PathFilters.ACCEPT_ALL_FILTER, true,
					subPath -> {
						CloudPath resultPath = (CloudPath)subPath.getResultPath();
						TreeNode<TrackedFileEntry> parent = getParentNode(lastProcessedNode.get(), resultPath);
						addTrackedFileEntry(parent, path.getFileSystem(), resultPath);
						return true;
					});
	}

	TreeNode<TrackedFileEntry> getParentNode(TreeNode<TrackedFileEntry> treeNode, CloudPath path) {
		CloudPath checkPath = Files.isDirectory(path) ? path : path.getParent();
		if (checkPath == null) {
			throw new RuntimeException("Unable to get parent of path " + path.toString());
		}

		TreeNode<TrackedFileEntry> checkNode = treeNode;
		while (!checkNode.data().getPath().equals(checkPath)) {
			checkNode = checkNode.parent();
			if (checkNode == null) {
				throw new RuntimeException("Unable to get parent of path " + path.toString() + " from directory hierarchy list");
			}
		}

		return checkNode;
	}

	boolean addTrackedFileEntry(TreeNode<TrackedFileEntry> parent, CloudFileSystem fileSystem, Path path) {
		if (Files.isDirectory(path)) {
			return false;
		}

		CloudFileAttributesView fileAttributeView =
				fileSystem.provider().getFileAttributeView(path, CloudFileAttributesView.class);

		if (fileAttributeView == null) {
			LOG.warn("No cloud file attributes available for path {}", path.toString());
		}

		CloudAclFileAttributes readAttributes;
		try {
			readAttributes = fileAttributeView.readAttributes();
		} catch (IOException e) {
			LOG.warn("Could not read file attributes for path {}", path.toString());
			return false;
		}

		HashCode contentMD5 = readAttributes.getContentMD5();
		if (contentMD5 == null) {
			LOG.warn("Cannot get content MD5 for path {}", path.toString());
		} else {
			contentMD5.toString();
		}

		return true;
	}

}