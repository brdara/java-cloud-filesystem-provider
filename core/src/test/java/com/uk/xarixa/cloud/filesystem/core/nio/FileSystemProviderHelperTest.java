package com.uk.xarixa.cloud.filesystem.core.nio;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper.DirectoryIterationPaths;

@RunWith(BlockJUnit4ClassRunner.class)
public class FileSystemProviderHelperTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};
	
	@Test
	public void testDirectoryIterationPathsReturnsARelativeSourcePath() {
		CloudFileSystem fileSystem = context.mock(CloudFileSystem.class);
		CloudPath containerPath = new CloudPath(fileSystem, true, "unit-test-container");
		CloudPath cpStart = new CloudPath(containerPath, "/root/start");
		CloudPath cpRelative = new CloudPath(containerPath, "/root/start/next/source");
		CloudPath cpResult = new CloudPath(containerPath, "/root/start/next/source/file.txt");
		String expectedRelativeSourcePath = "start/next/source";
		
        context.checking(new Expectations() {{
			allowing(fileSystem).getSeparator(); will(returnValue("/"));

			exactly(1).of(fileSystem).getPath(expectedRelativeSourcePath);
        	will(returnValue(new CloudPath(containerPath, expectedRelativeSourcePath)));
        }});

		DirectoryIterationPaths dirPaths = new DirectoryIterationPaths(cpStart, cpRelative, cpResult);
		Assert.assertEquals(expectedRelativeSourcePath, dirPaths.getRelativeSourcePath().get().toString());
	}

	@Test
	public void testDirectoryIterationPathsReturnsNoPathWhereThePathsAreEquibalent() {
		CloudFileSystem fileSystem = context.mock(CloudFileSystem.class);
		CloudPath containerPath = new CloudPath(fileSystem, true, "unit-test-container");
		CloudPath cpStart = new CloudPath(containerPath, "/root/start/next/source");
		CloudPath cpRelative = new CloudPath(containerPath, "/root/start/next/source");
		
		// When the start and relative paths are the same no path is returned
		DirectoryIterationPaths dirPaths = new DirectoryIterationPaths(cpStart, cpRelative, null);
		Assert.assertFalse(dirPaths.getRelativeSourcePath().isPresent());
	}

}
