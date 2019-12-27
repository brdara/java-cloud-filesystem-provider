package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudPathTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};
	
	@Test
	public void testCreateCloudPathWithASimpleRootWillThrowAnExceptionForAnInvalidRootPath() {
		try {
			new CloudPath((CloudFileSystem)null, true, "/");
			Assert.fail();
		} catch (CloudPathException e) {
			// OK;
		}
	}

	@Test
	public void testCreateCloudPathWithARootWhichIsZeroLengthWillIgnoreThisPartOfThePath() {
		Assert.assertEquals("/folder1", new CloudPath((CloudFileSystem)null, true, "//folder1").toString());
	}

	@Test
	public void testCreateCloudPathWithARootWhichIsAnEmptyStringWillThrowAnExceptionForAnInvalidRootPath() {
		try {
			new CloudPath((CloudFileSystem)null, true, "/  ");
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// OK;
		}
	}

	@Test
	public void testCreateCloudPathCreatesARealFullPathWithAValidRoot() {
		Assert.assertEquals("/root", new CloudPath((CloudFileSystem)null, true, "/root").toString());
		Assert.assertEquals("/root/folder1",
				new CloudPath((CloudFileSystem)null, true, "/root/folder1").toString());
		Assert.assertEquals("/root/folder1/folder2",
				new CloudPath((CloudFileSystem)null, true, "/root/folder1/folder2").toString());
		Assert.assertEquals("/root/folder1/blah.txt",
				new CloudPath((CloudFileSystem)null, true, "/root/folder1/folder2/../blah.txt").toString());
		Assert.assertEquals("/root/folder1",
				new CloudPath((CloudFileSystem)null, true, "/root/folder1/folder2/..").toString());
		Assert.assertEquals("/root",
				new CloudPath((CloudFileSystem)null, true, "/root/folder1/folder2/../..").toString());
		Assert.assertEquals("/root/blah.txt",
				new CloudPath((CloudFileSystem)null, true, "/root/folder1/folder2/../../blah.txt").toString());
	}
	
	@Test
	public void testCreateCloudPathWithARootPathCreatesAFullPath() {
		CloudPath rootPath = new CloudPath((CloudFileSystem)null, true, "/root");
		CloudPath subPath = new CloudPath((CloudFileSystem)null, false, rootPath, "subdir");
		Assert.assertEquals("/root/subdir", subPath.toAbsolutePath().toString());
	}

	@Test
	public void testGetRootWillReturnTheRootPathFromAnAbsolutePath() {
		Assert.assertNull(new CloudPath((CloudFileSystem)null, true, "/blah").getRoot());
		Assert.assertEquals("/blah", new CloudPath((CloudFileSystem)null, true, "/blah/fah").getRoot().toString());
		Assert.assertEquals("/blah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getRoot().toString());
	}
	
	@Test
	public void testGetRootWillReturnTheRootPathFromARelativePath() {
		CloudPath rootPath = new CloudPath((CloudFileSystem)null, true, "/blah");
		CloudPath relativePath = new CloudPath(rootPath, "/fah/wah");
		Assert.assertFalse(relativePath.isRootPath());
		Assert.assertFalse(relativePath.isAbsolute());
		CloudPath rootFromRelative = (CloudPath)relativePath.getRoot();
		Assert.assertNotNull(rootFromRelative);
		Assert.assertEquals(rootPath.getAllPaths(), rootFromRelative.getAllPaths());
	}
	
	@Test
	public void testGetFilenameWillReturnTheNameOfTheDirectoryOrFile() {
		Assert.assertEquals("hah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getFileName().toString());
		Assert.assertEquals("plank.txt", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").getFileName().toString());
		Assert.assertEquals("plank.txt", new CloudPath((CloudFileSystem)null, true, "/blah/plank.txt").getFileName().toString());
		Assert.assertEquals("blah", new CloudPath((CloudFileSystem)null, true, "/blah").getFileName().toString());
	}
	
	@Test
	public void testGetParentWillReturnTheParentPath() {
		Assert.assertNull(new CloudPath((CloudFileSystem)null, true, "/blah").getParent());
		Assert.assertEquals("/blah", new CloudPath((CloudFileSystem)null, true, "/blah/fah").getParent().toString());
		Assert.assertEquals("/blah/fah/wah/woo", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getParent().toString());
		Assert.assertEquals("/blah/fah/wah/woo/hah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").getParent().toString());
	}
	
	@Test
	public void testGetNameCountWillReturnTheNumberOfElementsInAPath() {
		Assert.assertEquals(0, new CloudPath((CloudFileSystem)null, true, "/blah").getNameCount());
		Assert.assertEquals(1, new CloudPath((CloudFileSystem)null, true, "/blah/fah").getNameCount());
		Assert.assertEquals(4, new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getNameCount());
		Assert.assertEquals(5, new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").getNameCount());
	}

	@Test
	public void testGetNameWillReturnAnIndexedName() {
		Assert.assertEquals("/blah", new CloudPath((CloudFileSystem)null, true, "/blah/fah").getName(0).toString());
		Assert.assertEquals("fah", new CloudPath((CloudFileSystem)null, true, "/blah/fah").getName(1).toString());

		Assert.assertEquals("/blah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getName(0).toString());
		Assert.assertEquals("fah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getName(1).toString());
		Assert.assertEquals("wah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getName(2).toString());
		Assert.assertEquals("woo", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getName(3).toString());
		Assert.assertEquals("hah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").getName(4).toString());
	}
	
	@Test
	public void testSubpathReturnsSubpathIndexedInTheFullPath() {
		Assert.assertEquals("wah/woo", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").subpath(2, 3).toString());
		Assert.assertEquals("wah/woo/hah", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah").subpath(2, 4).toString());
		Assert.assertEquals("woo/hah/plank.txt", new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").subpath(3, 5).toString());
	}

	@Test
	public void testStartsWithIdentifiesTheCorrectStartPath() {
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.startsWith(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah")));
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.startsWith(new CloudPath((CloudFileSystem)null, true, "/blah")));
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.startsWith(new CloudPath((CloudFileSystem)null, true, "/blah/")));
		Assert.assertFalse(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.startsWith(new CloudPath((CloudFileSystem)null, true, "/blah/fa")));
	}

	@Test
	public void testEndsWithIdentifiesTheCorrectEndPath() {
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.endsWith(new CloudPath((CloudFileSystem)null, true, "plank.txt")));
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.endsWith(new CloudPath((CloudFileSystem)null, true, "hah/plank.txt")));
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.endsWith(new CloudPath((CloudFileSystem)null, true, "woo/hah/plank.txt")));
		Assert.assertFalse(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")
				.endsWith(new CloudPath((CloudFileSystem)null, true, "woo/hah")));
	}
	
	@Test
	public void testGetCloudPathIteratorWillNotIterateOverTheRootNode() {
		Iterator<Path> iterator = new CloudPath((CloudFileSystem)null, true, "/blah").iterator();
		Assert.assertFalse(iterator.hasNext());
	}

	@Test
	public void testGetCloudPathIteratorIteratesOverAllButTheRootNode() {
		Iterator<Path> iterator = new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").iterator();
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals("fah", iterator.next().toString());
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals("wah", iterator.next().toString());
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals("woo", iterator.next().toString());
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals("hah", iterator.next().toString());
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals("plank.txt", iterator.next().toString());
	}
	
	@Test
	public void testToRealPathWillThrowAnExceptionIfTheFileDoesNotExist() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudFileSystemProviderDelegate fsProvider = context.mock(CloudFileSystemProviderDelegate.class);
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah/plank.txt";
		final CloudPath cp = new CloudPath(fs, true, "/" + container + "/" + filePath);

		context.checking(new Expectations() {{
			allowing(fs).provider();
			will(returnValue(fsProvider));

			atLeast(1).of(fsProvider).checkAccess(cp);
			will(throwException(new FileNotFoundException()));
		}});
		
		try {
			cp.toRealPath();
			Assert.fail("Expected an error to be thrown when the file doesn't exist on the remote Cloud");
		} catch (IOException e) {
			// OK
		}
	}

	@Test
	public void testToRealPathWillReturnThePathIfTheFileExists() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final CloudFileSystemProviderDelegate fsProvider = context.mock(CloudFileSystemProviderDelegate.class);
		final String container = "blah";
		final String filePath = "fah/wah/woo/hah/plank.txt";
		final CloudPath cp = new CloudPath(fs, true, "/" + container + "/" + filePath);

		context.checking(new Expectations() {{
			allowing(fs).provider();
			will(returnValue(fsProvider));

			atLeast(1).of(fsProvider).checkAccess(cp);

			atMost(1).of(fsProvider).readAttributes(
					with(cp), with(any(Class.class)), with(any(LinkOption[].class)));
			will(returnValue(null));
		}});
		
		Assert.assertEquals(cp, cp.toRealPath());
	}
	
	@Test
	public void testToRealPathWillThrowAnExceptionIfTheContainerDoesNotExist() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final String container = "blah";
		final CloudFileSystemProviderDelegate fsProvider = context.mock(CloudFileSystemProviderDelegate.class);
		final CloudPath cp = new CloudPath(fs, true, "/" + container);

		context.checking(new Expectations() {{
			allowing(fs).provider();
			will(returnValue(fsProvider));
			
			atLeast(1).of(fsProvider).checkAccess(cp);
			will(throwException(new FileNotFoundException()));
		}});
		
		try {
			cp.toRealPath();
			Assert.fail("Expected an error to be thrown when the container doesn't exist on the remote Cloud");
		} catch (IOException e) {
			// OK
		}
	}
	@Test
	public void testToRealPathWillReturnThePathForARootContainer() throws IOException {
		final CloudFileSystem fs = context.mock(CloudFileSystem.class);
		final String container = "blah";
		final CloudFileSystemProviderDelegate fsProvider = context.mock(CloudFileSystemProviderDelegate.class);
		final CloudPath cp = new CloudPath(fs, true, "/" + container);

		context.checking(new Expectations() {{
			allowing(fs).provider();
			will(returnValue(fsProvider));
			
			atLeast(1).of(fsProvider).checkAccess(cp);

			atMost(1).of(fsProvider).readAttributes(
					with(cp), with(any(Class.class)), with(any(LinkOption[].class)));
			will(returnValue(null));
		}});

		Assert.assertEquals(cp, cp.toRealPath());
	}

	@Test
	public void getContainerNameReturnsTheCloudContainerName() {
		Assert.assertEquals("blah",
				new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").getContainerName());
		Assert.assertEquals("blah",
				new CloudPath((CloudFileSystem)null, false, "/blah/fah/wah", "/woo/hah/plank.txt").getContainerName());
	}

	@Test
	public void getPathNameReturnsTheCloudPathWithoutTheContainerName() {
		Assert.assertEquals("fah/wah/woo/hah/plank.txt",
				new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").getPathName());
		Assert.assertEquals("fah/wah/woo/hah/plank.txt",
				new CloudPath((CloudFileSystem)null, false, "/blah/fah/wah", "/woo/hah/plank.txt").getPathName());
	}

//	@Test
//	public void testCompareToCannotCompareACloudPathAgainstAnotherPathType() throws IOException {
//		Path tempFilePath = Files.createTempFile("unit", "test");
//		
//		try {
//			new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt").compareTo(tempFilePath);
//			Assert.fail("Did not expect to be able to compare two different path types");
//		} catch (ClassCastException e) {
//			// OK
//		} finally {
//			Files.delete(tempFilePath);
//		}
//	}

	@Test
	public void testCompareToWillComparePathsLexicographically() throws IOException {
		CloudPath plankPath = new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt");
		Assert.assertEquals(-1, plankPath.getParent().compareTo(plankPath));
		Assert.assertEquals(1, plankPath.compareTo(plankPath.getParent()));
		Assert.assertEquals(0, plankPath.compareTo(plankPath));
		Assert.assertEquals(0, plankPath.compareTo(new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah/plank.txt")));
		Assert.assertEquals(1, plankPath.compareTo(new CloudPath((CloudFileSystem)null, true, "/blah/fah")));
		Assert.assertTrue(plankPath.compareTo(new CloudPath((CloudFileSystem)null, true, "/blah/fah/blah")) > 0);
		Assert.assertTrue(new CloudPath((CloudFileSystem)null, true, "/blah/fah/blah").compareTo(plankPath) < 0);
	}

	@Test
	public void testRegisterWithCloudWatchServiceRegistersADirectory() throws IOException {
		CloudWatchService watchService = context.mock(CloudWatchService.class);
		CloudPath dirPath = new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah");
		WatchEvent.Kind<?>[] watchEvents = new WatchEvent.Kind<?>[] {StandardWatchEventKinds.ENTRY_CREATE};
		WatchKey key = context.mock(WatchKey.class);

		context.checking(new Expectations() {{
			exactly(1).of(watchService).register(dirPath, watchEvents);
			will(returnValue(key));
		}});

		Assert.assertEquals(key, dirPath.register(watchService, watchEvents));
	}

	@Test
	public void testRegisterWithWatchServiceThrowsAnException() throws IOException {
		WatchService watchService = context.mock(WatchService.class);
		CloudPath dirPath = new CloudPath((CloudFileSystem)null, true, "/blah/fah/wah/woo/hah");
		WatchEvent.Kind<?>[] watchEvents = new WatchEvent.Kind<?>[] {StandardWatchEventKinds.ENTRY_CREATE};

		try {
			dirPath.register(watchService, watchEvents);
			Assert.fail("Expected an exception to be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}

}
