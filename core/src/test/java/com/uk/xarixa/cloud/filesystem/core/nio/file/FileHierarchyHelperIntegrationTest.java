package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scalified.tree.TreeNode;
import com.uk.xarixa.cloud.filesystem.core.AbstractJCloudsIntegrationTest;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.DefaultCloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

@RunWith(BlockJUnit4ClassRunner.class)
public class FileHierarchyHelperIntegrationTest extends AbstractJCloudsIntegrationTest {
	private final static Logger LOG = LoggerFactory.getLogger(FileHierarchyHelperIntegrationTest.class);
	private DefaultCloudFileSystemImplementation impl;

	@Override
	protected void postSetUp() {
		impl = new DefaultCloudFileSystemImplementation();
	}

	@Test
	public void testInspectCurrentStateReturnsATreeOfAllDirectoriesAndFiles() throws IOException {
		CloudPath root = new CloudPath(containerPath, "dir1");

		try {
			createDirectory(root);
			createRawContent("dir1/file1_1", "File 1_1".getBytes("UTF-8"));
			createRawContent("dir1/file1_2", "File 1_2".getBytes("UTF-8"));
			createDirectory(new CloudPath(containerPath, "dir1/dir1_2"));
			createDirectory(new CloudPath(containerPath, "dir1/dir1_2/dir_1_2_1"));
			createRawContent("dir1/dir1_2/dir_1_2_1/file_1_2_1_1", "File 1_2_1_1".getBytes("UTF-8"));
			createRawContent("dir1/dir1_2/dir_1_2_1/file_1_2_1_2", "File 1_2_1_2".getBytes("UTF-8"));
			createDirectory(new CloudPath(containerPath, "dir1/dir1_2/dir_1_2_2"));
			createDirectory(new CloudPath(containerPath, "dir1/dir1_3"));
			createRawContent("dir1/dir1_3/file3_1", "File 2_1".getBytes("UTF-8"));
			
			TreeNode<TrackedFileEntry> treeRoot = FileHierarchyHelper.getTrackedFileEntryTree(root, true);
			LOG.info("Tree hierarchy: {}", treeRoot);
			//FileHierarchyHelper.logTrackedFileEntryTreeHierarchy(treeRoot, "Current Hierarchy: {}");
		} finally {
			try {
				((CloudFileSystemProvider)provider).delete(root, DeleteOption.RECURSIVE);
			} catch (Exception e) {
				LOG.error("Cannot delete root", e);
			}
		}
	}

	void createDirectory(CloudPath dir) throws IOException {
		impl.createDirectory(blobStoreContext, dir);
		assertDirectoryExists(dir);
	}

	public void assertDirectoryExists(CloudPath cloudPath) throws IOException {
		Assert.assertTrue(impl.readAttributes(blobStoreContext, CloudBasicFileAttributes.class, cloudPath).isDirectory());
	}

}
