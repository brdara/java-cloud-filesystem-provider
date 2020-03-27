package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.util.Iterator;
import java.util.SortedSet;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.scalified.tree.TreeNode;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonResult;
import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonSide;

@RunWith(BlockJUnit4ClassRunner.class)
public class FileHierarchyHelperTest extends AbstractFileTestsHelper {

	@Override
	void preSetUp() {
	}

	@Override
	void postSetUp() {
	}

	@Test
	public void testGetParentNodeFromAHierarchyTraversesTheHierarchy() throws IOException {
		TreeNode<TrackedFileEntry> root = createTreeNodeForDirectoryPath("/root");
		TreeNode<TrackedFileEntry> folder1 = createTreeNodeForDirectoryPath("/root/folder1");
		TreeNode<TrackedFileEntry> folder_1_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1");
		TreeNode<TrackedFileEntry> folder_1_1_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1/folder_1_1_1");
		TreeNode<TrackedFileEntry> folder2 = createTreeNodeForDirectoryPath("/root/folder2");
		root.add(folder1);
		folder1.add(folder_1_1);
		folder_1_1.add(folder_1_1_1);
		root.add(folder2);
		
		TreeNode<TrackedFileEntry> parentNode5 = FileHierarchyHelper.getParentNode(root, createCloudPath("/root/file4.txt", false));
		Assert.assertEquals(root, parentNode5);

		TreeNode<TrackedFileEntry> parentNode1 = FileHierarchyHelper.getParentNode(root, createCloudPath("/root/folder1/folder_1_1/folder_1_1_1/file1.txt", false));
		Assert.assertEquals(folder_1_1_1, parentNode1);

		TreeNode<TrackedFileEntry> parentNode2 = FileHierarchyHelper.getParentNode(root, createCloudPath("/root/folder1/folder_1_1/folder_1_1_1", false));
		Assert.assertEquals(folder_1_1, parentNode2);

		TreeNode<TrackedFileEntry> parentNode3 = FileHierarchyHelper.getParentNode(root, createCloudPath("/root/folder1/folder_1_1/file2.txt", false));
		Assert.assertEquals(folder_1_1, parentNode3);

		TreeNode<TrackedFileEntry> parentNode4 = FileHierarchyHelper.getParentNode(root, createCloudPath("/root/folder1/file3.txt", false));
		Assert.assertEquals(folder1, parentNode4);
	}
	
	@Test
	public void testCompareFileTreeHierarchiesShowsNoEventsWhenTheHierarchiesAreEqual() throws IOException {
		TreeNode<TrackedFileEntry> root = createTreeNodeForDirectoryPath("/root");
		TreeNode<TrackedFileEntry> folder1 = createTreeNodeForDirectoryPath("/root/folder1");
		TreeNode<TrackedFileEntry> folder_1_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1");
		TreeNode<TrackedFileEntry> folder_1_1_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1/folder_1_1_1");
		TreeNode<TrackedFileEntry> folder2 = createTreeNodeForDirectoryPath("/root/folder2");
		root.add(folder1);
		folder1.add(folder_1_1);
		folder_1_1.add(folder_1_1_1);
		root.add(folder2);

		TreeNode<TrackedFileEntry> rootRhs = createTreeNodeForDirectoryPath("/root");
		TreeNode<TrackedFileEntry> folder1Rhs = createTreeNodeForDirectoryPath("/root/folder1");
		TreeNode<TrackedFileEntry> folder_1_1Rhs = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1");
		TreeNode<TrackedFileEntry> folder_1_1_1Rhs = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1/folder_1_1_1");
		TreeNode<TrackedFileEntry> folder2Rhs = createTreeNodeForDirectoryPath("/root/folder2");
		rootRhs.add(folder1Rhs);
		folder1Rhs.add(folder_1_1Rhs);
		folder_1_1Rhs.add(folder_1_1_1Rhs);
		rootRhs.add(folder2Rhs);
		
		CollatingBySideFileTreeComparisonEventHandler eventHandler = new CollatingBySideFileTreeComparisonEventHandler();
		FileHierarchyHelper.compareFileTreeHierarchies(eventHandler, root, rootRhs);
		Assert.assertTrue("Unexpected events found: " + eventHandler.getEventsForSide(ComparisonSide.BOTH), eventHandler.getEventsForSide(ComparisonSide.BOTH).isEmpty());
		Assert.assertTrue("Unexpected events found: " + eventHandler.getEventsForSide(ComparisonSide.LHS), eventHandler.getEventsForSide(ComparisonSide.LHS).isEmpty());
		Assert.assertTrue("Unexpected events found: " + eventHandler.getEventsForSide(ComparisonSide.RHS), eventHandler.getEventsForSide(ComparisonSide.RHS).isEmpty());
	}

	@Test
	public void testCompareFileTreeHierarchiesShowsEventsWhenTheHierarchiesAreNotEqual() throws IOException {
		TreeNode<TrackedFileEntry> root = createTreeNodeForDirectoryPath("/root");
		TreeNode<TrackedFileEntry> folder1 = createTreeNodeForDirectoryPath("/root/folder1");
		TreeNode<TrackedFileEntry> folder_1_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1");
		TreeNode<TrackedFileEntry> folder_1_1_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1/folder_1_1_1");
		TreeNode<TrackedFileEntry> folder_1_2 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_2");
		TreeNode<TrackedFileEntry> folder_1_2_file_1_2_1 = createTreeNodeForDirectoryPath("/root/folder1/folder_1_2/file_1_2_1.txt");	// File is actually a folder here
		TreeNode<TrackedFileEntry> folder_1_2_file_1_2_2 = createTreeNodeForFilePath("/root/folder1/folder_1_2/file_1_2_2.txt", "6629fae49393a05397450978507c4ef1");	// File here
		TreeNode<TrackedFileEntry> folder_1_2_file_1_2_3 = createTreeNodeForFilePath("/root/folder1/folder_1_2/file_1_2_3.txt", "5ccc069c403ebaf9f0171e9517f40e41");	// File here
		TreeNode<TrackedFileEntry> folder2 = createTreeNodeForDirectoryPath("/root/folder2");
		root.add(folder1);
		folder1.add(folder_1_1);
		folder1.add(folder_1_2);
		folder_1_1.add(folder_1_1_1);
		folder_1_2.add(folder_1_2_file_1_2_1);
		folder_1_2.add(folder_1_2_file_1_2_2);
		folder_1_2.add(folder_1_2_file_1_2_3);
		root.add(folder2);

		TreeNode<TrackedFileEntry> rootRhs = createTreeNodeForDirectoryPath("/root");
		TreeNode<TrackedFileEntry> folder1Rhs = createTreeNodeForDirectoryPath("/root/folder1");
		TreeNode<TrackedFileEntry> folder_1_1Rhs = createTreeNodeForDirectoryPath("/root/folder1/folder_1_1");
		// This has missing /root/folder1/folder_1_1/folder_1_1_1 which should result in /root/folder1/folder_1_1 having MISSING_CHILDREN on the RHS as the entire folder is empty on the right
		TreeNode<TrackedFileEntry> folder_1_2Rhs = createTreeNodeForDirectoryPath("/root/folder1/folder_1_2");
		TreeNode<TrackedFileEntry> folder_1_2_file_1_2_1Rhs = createTreeNodeForFilePath("/root/folder1/folder_1_2/file_1_2_1.txt", "dcd98b7102dd2f0e8b11d0f600bfb0c093");	// File here so it should result in DIRECTORY_IS_FILE_MISMATCH for the LHS
		TreeNode<TrackedFileEntry> folder_1_2_file_1_2_2Rhs = createTreeNodeForFilePath("/root/folder1/folder_1_2/file_1_2_2.txt", "6629fae49393a05397450978507c4ef1");	// File here
		TreeNode<TrackedFileEntry> folder_1_2_file_1_2_3Rhs = createTreeNodeForFilePath("/root/folder1/folder_1_2/file_1_2_3.txt", "939e7578ed9e3c518a452acee763bce9");	// File here, checksums differ so will result in FILE_DIGEST_MISMATCH in the BOTH bucket
		// This has missing /root/folder2 which should result in it being MISSING_OTHER_SIDE for the LHS
		rootRhs.add(folder1Rhs);
		folder1Rhs.add(folder_1_1Rhs);
		folder1Rhs.add(folder_1_2Rhs);
		folder_1_2Rhs.add(folder_1_2_file_1_2_1Rhs);
		folder_1_2Rhs.add(folder_1_2_file_1_2_2Rhs);
		folder_1_2Rhs.add(folder_1_2_file_1_2_3Rhs);

		CollatingByPathFileTreeComparisonEventHandler eventHandler = new CollatingByPathFileTreeComparisonEventHandler();
		FileHierarchyHelper.compareFileTreeHierarchies(eventHandler, root, rootRhs);

		SortedSet<FileTreeComparisonEvent> events = eventHandler.getEvents();
		Assert.assertEquals("Unexpected events found: " + events, 4, events.size());
		Iterator<FileTreeComparisonEvent> eventsIter = events.iterator();
		assertFileTreeComparisonEventEquals(ComparisonSide.RHS, ComparisonResult.MISSING_CHILDREN,
				folder_1_1.data().getPath(), folder_1_1Rhs.data().getPath(), eventsIter.next());		
		assertFileTreeComparisonEventEquals(ComparisonSide.LHS, ComparisonResult.DIRECTORY_IS_FILE_MISMATCH,
				folder_1_2_file_1_2_1.data().getPath(), folder_1_2_file_1_2_1Rhs.data().getPath(), eventsIter.next());
		assertFileTreeComparisonEventEquals(ComparisonSide.BOTH, ComparisonResult.FILE_DIGEST_MISMATCH,
				folder_1_2_file_1_2_3.data().getPath(), folder_1_2_file_1_2_3Rhs.data().getPath(), eventsIter.next());
		assertFileTreeComparisonEventEquals(ComparisonSide.LHS, ComparisonResult.MISSING_OTHER_SIDE,
				folder2.data().getPath(), rootRhs.data().getPath(), eventsIter.next());
	}

	private void assertFileTreeComparisonEventEquals(ComparisonSide expectedSide, ComparisonResult expectedResult,
			Path expectedLeftPath, Path expectedRightPath, FileTreeComparisonEvent actualEvent) {
		Assert.assertEquals("Event does not match: " + actualEvent, expectedSide, actualEvent.getSide());
		Assert.assertEquals("Event does not match: " + actualEvent, expectedResult, actualEvent.getResult());
		Assert.assertEquals("Event does not match: " + actualEvent, expectedLeftPath, actualEvent.getLeftPath());
		Assert.assertEquals("Event does not match: " + actualEvent, expectedRightPath, actualEvent.getRightPath());
	}
	
	public void testFileTreeComparisonEventToWatchEventKind() throws IOException {
		Assert.assertEquals(StandardWatchEventKinds.ENTRY_DELETE,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1/folder_1_1", true), createCloudPath("/root/folder1", true), 
								ComparisonSide.LHS, ComparisonResult.MISSING_OTHER_SIDE)));
		Assert.assertEquals(StandardWatchEventKinds.ENTRY_DELETE,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1", true), createCloudPath("/root/folder1/folder_1_1", true), 
								ComparisonSide.RHS, ComparisonResult.MISSING_OTHER_SIDE)));

		Assert.assertEquals(StandardWatchEventKinds.ENTRY_CREATE,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1", true), createCloudPath("/root/folder1/folder_1_1", true), 
								ComparisonSide.LHS, ComparisonResult.MISSING_CHILDREN)));
		Assert.assertEquals(StandardWatchEventKinds.ENTRY_CREATE,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1/folder_1_1", true), createCloudPath("/root/folder1", true), 
								ComparisonSide.RHS, ComparisonResult.MISSING_CHILDREN)));

		Assert.assertEquals(StandardWatchEventKinds.ENTRY_MODIFY,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1/file1_1.txt", false), createCloudPath("/root/folder1/file1_1.txt", false), 
								ComparisonSide.LHS, ComparisonResult.FILE_DIGEST_MISMATCH)));
		Assert.assertEquals(StandardWatchEventKinds.ENTRY_MODIFY,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1/file1_1.txt", false), createCloudPath("/root/folder1/file1_1.txt", false), 
								ComparisonSide.RHS, ComparisonResult.FILE_DIGEST_MISMATCH)));

		Assert.assertEquals(StandardWatchEventKinds.ENTRY_MODIFY,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1/file_1_1.txt", true), createCloudPath("/root/folder1/file1_1.txt", false), 
								ComparisonSide.LHS, ComparisonResult.DIRECTORY_IS_FILE_MISMATCH)));
		Assert.assertEquals(StandardWatchEventKinds.ENTRY_MODIFY,
				FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(
						new FileTreeComparisonEvent(createCloudPath("/root/folder1/file_1_1.txt", true), createCloudPath("/root/folder1/file1_1.txt", false), 
								ComparisonSide.RHS, ComparisonResult.DIRECTORY_IS_FILE_MISMATCH)));
	}


}
