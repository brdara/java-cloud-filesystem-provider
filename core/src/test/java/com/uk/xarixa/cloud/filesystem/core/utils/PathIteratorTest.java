package com.uk.xarixa.cloud.filesystem.core.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.utils.PathIterator;

@RunWith(BlockJUnit4ClassRunner.class)
public class PathIteratorTest {

	@Test
	public void testWillNotIterateOverAnEmptyPath() {
		try {
			new PathIterator("/");
			Assert.fail("Should not be able to iterate over an empty path");
		} catch (IllegalArgumentException e) {
		}

		try {
			new PathIterator(null);
			Assert.fail("Should not be able to iterate over an empty path");
		} catch (IllegalArgumentException e) {
		}

		try {
			new PathIterator("");
			Assert.fail("Should not be able to iterate over an empty path");
		} catch (IllegalArgumentException e) {
		}
	}
	
	@Test
	public void testIteratingOverAllPaths() {
		PathIterator rootPath = new PathIterator("/blah");
		Assert.assertTrue(rootPath.hasNext());
		Assert.assertEquals("blah", rootPath.next());
		Assert.assertFalse(rootPath.hasNext());

		PathIterator filePath = new PathIterator("/blah/fah/wah/woo/hah/plank.txt");
		Assert.assertTrue(filePath.hasNext());
		Assert.assertEquals("blah", filePath.next());
		Assert.assertEquals("fah", filePath.next());
		Assert.assertEquals("wah", filePath.next());
		Assert.assertEquals("woo", filePath.next());
		Assert.assertEquals("hah", filePath.next());
		Assert.assertEquals("plank.txt", filePath.next());
	}

}
