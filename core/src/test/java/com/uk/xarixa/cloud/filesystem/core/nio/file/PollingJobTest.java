package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.atomic.AtomicInteger;

import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.scalified.tree.TreeNode;
import com.scalified.tree.multinode.ArrayMultiTreeNode;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

@RunWith(BlockJUnit4ClassRunner.class)
public class PollingJobTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	private final AtomicInteger pathCounter = new AtomicInteger(0);
	private PollingJob pollingJob;
	private FileSystemProvider fsProvider;
	private CloudFileSystem fs;
	
	@Before
	public void setUp() {
		fs = context.mock(CloudFileSystem.class);
		fsProvider = context.mock(FileSystemProvider.class);
		pollingJob = new PollingJob();
		
		context.checking(new Expectations() {{
			allowing(fs).provider(); will(returnValue(fsProvider));
		}});
	}

	@Test
	public void testGetParentNodeFromAHierarchyTraversesTheHierarchy() throws IOException {
		TreeNode<TrackedFileEntry> root = createTreeNodeForDirectoryPath("/root");
		TreeNode<TrackedFileEntry> folder1 = createTreeNodeForDirectoryPath("/root/folder1");
		TreeNode<TrackedFileEntry> folder1_2 = createTreeNodeForDirectoryPath("/root/folder1/folder1_2");
		TreeNode<TrackedFileEntry> folder1_2_3 = createTreeNodeForDirectoryPath("/root/folder1/folder1_2_3");
		TreeNode<TrackedFileEntry> folder2 = createTreeNodeForDirectoryPath("/root/folder2");
		root.add(folder1);
		folder1.add(folder1_2);
		folder1_2.add(folder1_2_3);
		root.add(folder2);
		
		TreeNode<TrackedFileEntry> parentNode5 = pollingJob.getParentNode(folder1_2_3, createCloudPath("/root/file4.txt", false));
		Assert.assertEquals(root, parentNode5);

		TreeNode<TrackedFileEntry> parentNode1 = pollingJob.getParentNode(folder1_2_3, createCloudPath("/root/folder1/folder1_2_3/file1.txt", false));
		Assert.assertEquals(folder1_2_3, parentNode1);

		TreeNode<TrackedFileEntry> parentNode2 = pollingJob.getParentNode(folder1_2_3, createCloudPath("/root/folder1/folder1_2_3", false));
		Assert.assertEquals(folder1_2_3, parentNode2);

		TreeNode<TrackedFileEntry> parentNode3 = pollingJob.getParentNode(folder1_2_3, createCloudPath("/root/folder1/folder1_2/file2.txt", false));
		Assert.assertEquals(folder1_2, parentNode3);

		TreeNode<TrackedFileEntry> parentNode4 = pollingJob.getParentNode(folder1_2_3, createCloudPath("/root/folder1/file3.txt", false));
		Assert.assertEquals(folder1, parentNode4);
	}
	
	private TreeNode<TrackedFileEntry> createTreeNodeForDirectoryPath(String path) throws IOException {
		CloudPath cloudPath = createCloudPath(path, true);
		TrackedFileEntry entry = new TrackedFileEntry(cloudPath, true);
		return new ArrayMultiTreeNode<>(entry);
	}
	
	private CloudPath createCloudPath(String path, boolean isDirectory) throws IOException {
		CloudPath cloudPath = new CloudPath(fs, true, path);
		BasicFileAttributes fileAttrMock = context.mock(BasicFileAttributes.class, pathCounter.incrementAndGet() + "-" + path);
		
		context.checking(new Expectations() {{
			allowing(fsProvider).readAttributes(cloudPath, BasicFileAttributes.class);
			will(returnValue(fileAttrMock));
			
			allowing(fileAttrMock).isDirectory();
			will(returnValue(isDirectory));
		}});

		return cloudPath;
	}

}
