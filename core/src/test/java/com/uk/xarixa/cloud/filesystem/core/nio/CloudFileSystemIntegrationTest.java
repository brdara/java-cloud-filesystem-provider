package com.uk.xarixa.cloud.filesystem.core.nio;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.AbstractJCloudsIntegrationTest;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudFileSystemIntegrationTest extends AbstractJCloudsIntegrationTest {
	private CloudFileSystem impl;
	private String[] containers = new String[] {"com.uk.xarixa.cloud.filesystem.nio-test-container1", "com.uk.xarixa.cloud.filesystem.nio-test-container2"};

	@Override
	protected void postSetUp() {
		impl = new CloudFileSystem(provider, cloudHostSettings, blobStoreContext);
		Arrays.stream(containers).forEach(c ->
			Assert.assertTrue("Could not create container " + c,
				blobStoreContext.getBlobStore().createContainerInLocation(location, c)));
	}
	
	@Override
	protected void postTearDown() {
		Arrays.stream(containers).forEach(c ->
			blobStoreContext.getBlobStore().deleteContainer(c));
	}
	
	@Test
	public void testGetRootDirectoriesWillReturnAllContainers() {
		List<String> pathNames = new ArrayList<>();
		impl.getRootDirectories().forEach(d -> pathNames.add(d.toAbsolutePath().toString()));
		Assert.assertEquals(4, pathNames.size());
		Assert.assertTrue("Could not find path name " + CONTAINER_NAME + ": " + pathNames,
				pathNames.contains(CloudPath.DEFAULT_PATH_SEPARATOR + CONTAINER_NAME));
		Arrays.stream(containers).forEach(c ->
			Assert.assertTrue("Could not find path name " + c + ": " + pathNames,
					pathNames.contains(CloudPath.DEFAULT_PATH_SEPARATOR + c)));
	}

}
