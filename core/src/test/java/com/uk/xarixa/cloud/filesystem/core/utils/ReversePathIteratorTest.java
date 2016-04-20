package com.uk.xarixa.cloud.filesystem.core.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.utils.ReversePathIterator;

@RunWith(BlockJUnit4ClassRunner.class)
public class ReversePathIteratorTest {

	@Test
	public void testWillNotIterateOverAnEmptyPath() {
		try {
			new ReversePathIterator("/");
			Assert.fail("Should not be able to iterate over an empty path");
		} catch (IllegalArgumentException e) {
		}

		try {
			new ReversePathIterator(null);
			Assert.fail("Should not be able to iterate over an empty path");
		} catch (IllegalArgumentException e) {
		}

		try {
			new ReversePathIterator("");
			Assert.fail("Should not be able to iterate over an empty path");
		} catch (IllegalArgumentException e) {
		}
	}
	
	@Test
	public void testIteratingOverAllPaths() {
		ReversePathIterator rootPath = new ReversePathIterator("/blah");
		Assert.assertTrue(rootPath.hasNext());
		Assert.assertEquals("blah", rootPath.next());
		Assert.assertFalse(rootPath.hasNext());

		ReversePathIterator filePath = new ReversePathIterator("/blah/fah/wah/woo/hah/plank.txt");
		Assert.assertTrue(filePath.hasNext());
		Assert.assertEquals("plank.txt", filePath.next());
		Assert.assertEquals("hah", filePath.next());
		Assert.assertEquals("woo", filePath.next());
		Assert.assertEquals("wah", filePath.next());
		Assert.assertEquals("fah", filePath.next());
		Assert.assertEquals("blah", filePath.next());
	}

}
