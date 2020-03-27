package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent.Kind;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;
import com.scalified.tree.TreeNode;
import com.scalified.tree.multinode.ArrayMultiTreeNode;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonResult;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonSide;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

public final class FileHierarchyHelper {
	private static final Logger LOG = LoggerFactory.getLogger(PollingWatchServiceJob.class);
	private static PathOnlyComparator pathOnlyComparator = new PathOnlyComparator();

	static public class PathOnlyComparator implements Comparator<TrackedFileEntry> {

		@Override
		public int compare(TrackedFileEntry o1, TrackedFileEntry o2) {
			return o1.getPath().compareTo(o2.getPath());
		}
		
	}

	private FileHierarchyHelper() {}

	public static Kind<Path> fileTreeComparisonEventToWatchEventKind(FileTreeComparisonEvent event) {
		switch (event.getResult()) {
			case DIRECTORY_IS_FILE_MISMATCH: return StandardWatchEventKinds.ENTRY_MODIFY;
			case FILE_DIGEST_MISMATCH: return StandardWatchEventKinds.ENTRY_MODIFY;
			case MISSING_CHILDREN: return (event.getSide() == ComparisonSide.RHS ? StandardWatchEventKinds.ENTRY_DELETE : StandardWatchEventKinds.ENTRY_CREATE);
			case MISSING_OTHER_SIDE: return (event.getSide() == ComparisonSide.RHS ? StandardWatchEventKinds.ENTRY_CREATE : StandardWatchEventKinds.ENTRY_DELETE);
			default: return null;
		}
	}

	/**
	 * Compares two file trees which can be obtained via {@link #getTrackedFileEntryTree(Path)}
	 */
	public static void compareFileTreeHierarchies(FileTreeComparisonEventHandler eventHandler,
			TreeNode<TrackedFileEntry> treeLeft, TreeNode<TrackedFileEntry> treeRight) {
		compareFileTreeHierarchies(eventHandler, pathOnlyComparator, treeLeft, treeRight);
	}

	/**
	 * Compares two file trees which can be obtained via {@link #getTrackedFileEntryTree(Path)}
	 */
	public static void compareFileTreeHierarchies(FileTreeComparisonEventHandler eventHandler,
			Comparator<TrackedFileEntry> trackedFileEntryComparator,
			TreeNode<TrackedFileEntry> treeLeft, TreeNode<TrackedFileEntry> treeRight) {
		Collection<? extends TreeNode<TrackedFileEntry>> subtreesLeft = treeLeft.subtrees();
		Collection<? extends TreeNode<TrackedFileEntry>> subtreesRight = treeRight.subtrees();
		Set<TreeNode<TrackedFileEntry>> foundRightList = new HashSet<>();

		// Compare left with right
		for (TreeNode<TrackedFileEntry> leftNode : subtreesLeft) {
			Iterator<? extends TreeNode<TrackedFileEntry>> iter = subtreesRight.iterator();
			Optional<TreeNode<TrackedFileEntry>> foundRight = Optional.empty();

			while (iter.hasNext() && foundRight.isEmpty()) {
				TreeNode<TrackedFileEntry> rightNode = iter.next();

				if (trackedFileEntryComparator.compare(leftNode.data(), rightNode.data()) == 0) {
					// Found a matching path, check it's content is equivalent
					foundRight = Optional.of(rightNode);

					if (rightNode.data().isFolder() != leftNode.data().isFolder()) {
						eventHandler.handleEvent(new FileTreeComparisonEvent(leftNode.data().getPath(), rightNode.data().getPath(),
								rightNode.data().isFolder() ? ComparisonSide.RHS : ComparisonSide.LHS, ComparisonResult.DIRECTORY_IS_FILE_MISMATCH));
					} else if (!leftNode.data().isFolder() && !fileChecksumsEqual(leftNode, rightNode)) {
						eventHandler.handleEvent(new FileTreeComparisonEvent(leftNode.data().getPath(), rightNode.data().getPath(),
								ComparisonSide.BOTH, ComparisonResult.FILE_DIGEST_MISMATCH));
					}
				}
			}

			if (foundRight.isEmpty()) {
				// This was not found on the right side
				eventHandler.handleEvent(new FileTreeComparisonEvent(leftNode.data().getPath(), treeRight.data().getPath(),
						ComparisonSide.LHS, ComparisonResult.MISSING_OTHER_SIDE));
			} else {
				// Add it to our found list and then compare the hierarchies...
				foundRightList.add(foundRight.get());

				// Test if there are children in both
				if (leftNode.isLeaf() != foundRight.get().isLeaf()) {
					eventHandler.handleEvent(new FileTreeComparisonEvent(leftNode.data().getPath(), foundRight.get().data().getPath(),
							leftNode.isLeaf() ? ComparisonSide.LHS : ComparisonSide.RHS, ComparisonResult.MISSING_CHILDREN));
				} else {
					compareFileTreeHierarchies(eventHandler, trackedFileEntryComparator, leftNode, foundRight.get());
				}
			}
		}

		// Check for nodes in the right which are not found on the left
		subtreesRight.stream().filter(rightNode -> !foundRightList.contains(rightNode))
			.forEach(rightNode -> {
				// So we know this one doesn't exist on the left side as it was previously not found
				eventHandler.handleEvent(new FileTreeComparisonEvent(rightNode.data().getPath(), treeLeft.data().getPath(),
						ComparisonSide.RHS, ComparisonResult.MISSING_OTHER_SIDE));
			});
	}

	/**
	 * Checks if the file checksums are equal
	 */
	private static boolean fileChecksumsEqual(TreeNode<TrackedFileEntry> leftNode, TreeNode<TrackedFileEntry> rightNode) {
		if (StringUtils.isBlank(leftNode.data().getCheckSum()) && !StringUtils.isBlank(rightNode.data().getCheckSum())) {
			return false;
		}

		if (!StringUtils.isBlank(leftNode.data().getCheckSum()) && StringUtils.isBlank(rightNode.data().getCheckSum())) {
			return false;
		}
		
		return StringUtils.equals(leftNode.data().getCheckSum(), rightNode.data().getCheckSum());
	}

	/**
	 * Walks the entire file tree from the root path and creates a tree of entries
	 * @param path
	 * @return
	 */
	public static TreeNode<TrackedFileEntry> getTrackedFileEntryTree(Path path, boolean recursive) {
		final TreeNode<TrackedFileEntry> root = new ArrayMultiTreeNode<>(new TrackedFileEntry(path.toAbsolutePath()));
		//final AtomicReference<TreeNode<TrackedFileEntry>> lastProcessedNode = new AtomicReference<>(root);

		FileSystemProviderHelper.iterateOverDirectoryContents(path.getFileSystem(), Optional.ofNullable(path),
				PathFilters.ACCEPT_ALL_FILTER, recursive,
					subPath -> {
						Path resultPath = subPath.getResultPath().toAbsolutePath();
						TreeNode<TrackedFileEntry> parent = getParentNode(root, resultPath);
						if (parent == null) {
							LOG.error("Could not get parent of path {} in hierarchy: {}", root, resultPath.toString());
							throw new RuntimeException("Unable to get parent of path " + resultPath.toString() + " in hierarchy");
						}
						addTrackedFileEntry(parent, path.getFileSystem(), resultPath);
						return true;
					});

		return root;
	}

	/**
	 * Gets the parent node for a path in the tree from the <em>root</em>
	 * @param root	Root node to search from
	 * @param path	A path whose parent is to be searched in the tree
	 * @return
	 */
	public static TreeNode<TrackedFileEntry> getParentNode(TreeNode<TrackedFileEntry> root, Path path) {
		Path checkPath = path.getParent();
		if (checkPath == null) {
			throw new RuntimeException("Unable to get parent of path " + path.toString());
		}

		return getNode(root, checkPath);
	}

	/**
	 * Gets the node for a given path in the tree from the <em>root</em>
	 * @param root	Root node to search from
	 * @param path	A path to be searched for in the tree
	 * @return
	 */
	public static TreeNode<TrackedFileEntry> getNode(TreeNode<TrackedFileEntry> root, Path path) {
		TrackedFileEntry searchPath = new TrackedFileEntry(path);
		return root.find(searchPath);
	}

	static boolean addTrackedFileEntry(TreeNode<TrackedFileEntry> parent, FileSystem fileSystem, Path path) {
		TrackedFileEntry entry;

		if (!Files.isDirectory(path)) {
			entry = new TrackedFileEntry(path, false);

			CloudFileAttributesView fileAttributeView =
					fileSystem.provider().getFileAttributeView(path, CloudFileAttributesView.class);
	
			if (fileAttributeView == null) {
				LOG.debug("No cloud file attributes available for path {}, defaulting to calculating MD5 checksum", path.toString());
				calculateMd5ChecksumManually(path, entry);
			} else {
	
				CloudAclFileAttributes readAttributes = null;
				try {
					readAttributes = fileAttributeView.readAttributes();
				} catch (IOException e) {
					LOG.warn("Could not read file attributes for path {}", path.toString());
					calculateMd5ChecksumManually(path, entry);
				}

				if (readAttributes != null) {
					HashCode contentMD5 = readAttributes.getContentMD5();
					if (contentMD5 == null) {
						LOG.warn("Cannot get content MD5 for path {}", path.toString());
						calculateMd5ChecksumManually(path, entry);
					} else {
						entry.setCheckSum(contentMD5.toString());
					}
				}
			}
		} else {
			entry = new TrackedFileEntry(path, true);
		}
		
		parent.add(new ArrayMultiTreeNode<>(entry));
		return entry.isFolder();
	}

	/**
	 * Fallback to calculate the MD5 checksum through {@link FileSystemProviderHelper#calculateMD5Checksum(Path)}
	 * @param path
	 * @param entry
	 */
	private static void calculateMd5ChecksumManually(Path path, TrackedFileEntry entry) {
		try {
			entry.setCheckSum(FileSystemProviderHelper.calculateMD5Checksum(path));
		} catch (IOException e) {
			LOG.warn("Could not get MD5 checksum for path {}", path, e);
		}
	}

}
