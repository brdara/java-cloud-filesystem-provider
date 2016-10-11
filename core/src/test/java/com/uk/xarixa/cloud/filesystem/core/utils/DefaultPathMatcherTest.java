package com.uk.xarixa.cloud.filesystem.core.utils;

import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.utils.DefaultPathMatcher;

@RunWith(BlockJUnit4ClassRunner.class)
public class DefaultPathMatcherTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};

	@Test
	public void testExtractRegexFromGlobPatternFailsIfAGroupingIsNotFound() {
		try {
			DefaultPathMatcher.extractRegexFromGlobPattern("*.{java,class", CloudPath.DEFAULT_PATH_SEPARATOR);
			Assert.fail("Expected an exception to be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}

	@Test
	public void testExtractRegexFromGlobPatternFailsIfABracketsEndingIsNotFound() {
		try {
			DefaultPathMatcher.extractRegexFromGlobPattern("*.[a-z", CloudPath.DEFAULT_PATH_SEPARATOR);
			Assert.fail("Expected an exception to be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}

	@Test
	public void testExtractRegexFromGlobPatternParsesCorrectly() {
		Assert.assertEquals("[^/]*\\.java", DefaultPathMatcher.extractRegexFromGlobPattern("*.java", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals(".*\\.java", DefaultPathMatcher.extractRegexFromGlobPattern("**.java", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals(".*/[^/]*\\.java", DefaultPathMatcher.extractRegexFromGlobPattern("**/*.java", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals("/home/.*", DefaultPathMatcher.extractRegexFromGlobPattern("/home/**", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals("/home/gus/.*", DefaultPathMatcher.extractRegexFromGlobPattern("/home/gus/**", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals("/home/gus/[^/]*", DefaultPathMatcher.extractRegexFromGlobPattern("/home/gus/*", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals("file\\.[^/][^/][^/]", DefaultPathMatcher.extractRegexFromGlobPattern("file.???", CloudPath.DEFAULT_PATH_SEPARATOR));
		Assert.assertEquals("special\\@chars\\.\\$\\$\\$", DefaultPathMatcher.extractRegexFromGlobPattern("special\\@chars.$$$", CloudPath.DEFAULT_PATH_SEPARATOR));
	}

	@Test
	public void testConstructorCreatesANewPatternMatcherFromASimpleGlobExpression() {
		DefaultPathMatcher matcher = new DefaultPathMatcher("glob:*.java");
		Assert.assertTrue(matcher.matches("DefaultPathMatcherTest.java"));
		Assert.assertFalse(matcher.matches("/DefaultPathMatcherTest.java"));
		Assert.assertFalse(matcher.matches("/dir/DefaultPathMatcherTest.java"));
	}

	@Test
	public void testConstructorCreatesANewPatternMatcherFromASimpleGlobExpressionWithSpecialCharacters() {
		DefaultPathMatcher matcher = new DefaultPathMatcher("glob:*xx.$$$");
		Assert.assertTrue(matcher.matches("somexx.$$$"));
		Assert.assertFalse(matcher.matches("/somexx.$$$"));
		Assert.assertFalse(matcher.matches("/dir/somexx.$$$"));
	}

	@Test
	public void testConstructorCreatesANewPatternMatcherFromAComplexGlobExpressionWithDirectories() {
		DefaultPathMatcher matcher = new DefaultPathMatcher("glob:/home/users/**/*efault*.java");
		Assert.assertFalse(matcher.matches("DefaultPathMatcherTest.java"));
		Assert.assertFalse(matcher.matches("/DefaultPathMatcherTest.java"));
		Assert.assertFalse(matcher.matches("/home/users/DefaultPathMatcherTest.java"));
		Assert.assertTrue(matcher.matches("/home/users/userdir/DefaultPathMatcherTest.java"));
	}
	
	@Test
	public void testConstructorCreatesANewPatternMatcherFromAComplexGlobExpressionWithGroupings() {
		DefaultPathMatcher matcher = new DefaultPathMatcher("glob:**/*.{java,properties,xml}");
		Assert.assertFalse(matcher.matches("DefaultPathMatcherTest.java"));
		Assert.assertTrue(matcher.matches("/DefaultPathMatcherTest.java"));
		Assert.assertTrue(matcher.matches("/home/users/DefaultPathMatcherTest.java"));
		Assert.assertTrue(matcher.matches("/home/users/userdir/DefaultPathMatcherTest.java"));
	}

}
