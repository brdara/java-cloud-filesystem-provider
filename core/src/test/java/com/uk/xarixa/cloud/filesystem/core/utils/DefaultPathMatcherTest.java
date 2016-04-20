package com.uk.xarixa.cloud.filesystem.core.utils;

import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

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
			DefaultPathMatcher.extractRegexFromGlobPattern("*.{java,class");
			Assert.fail("Expected an exception to be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}

	@Test
	public void testExtractRegexFromGlobPatternFailsIfABracketsEndingIsNotFound() {
		try {
			DefaultPathMatcher.extractRegexFromGlobPattern("*.[a-z");
			Assert.fail("Expected an exception to be thrown");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}

	@Test
	public void testExtractRegexFromGlobPatternParsesCorrectly() {
		Assert.assertEquals("[^/]*\\.java", DefaultPathMatcher.extractRegexFromGlobPattern("*.java"));
		Assert.assertEquals(".*\\.java", DefaultPathMatcher.extractRegexFromGlobPattern("**.java"));
		Assert.assertEquals(".*/[^/]*\\.java", DefaultPathMatcher.extractRegexFromGlobPattern("**/*.java"));
		Assert.assertEquals("/home/.*", DefaultPathMatcher.extractRegexFromGlobPattern("/home/**"));
		Assert.assertEquals("/home/gus/.*", DefaultPathMatcher.extractRegexFromGlobPattern("/home/gus/**"));
		Assert.assertEquals("/home/gus/[^/]*", DefaultPathMatcher.extractRegexFromGlobPattern("/home/gus/*"));
		Assert.assertEquals("file\\.[^/][^/][^/]", DefaultPathMatcher.extractRegexFromGlobPattern("file.???"));
		Assert.assertEquals("special\\@chars\\.\\$\\$\\$", DefaultPathMatcher.extractRegexFromGlobPattern("special\\@chars.$$$"));
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

}
