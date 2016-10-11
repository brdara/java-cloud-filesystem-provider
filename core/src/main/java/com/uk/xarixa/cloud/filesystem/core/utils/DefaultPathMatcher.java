package com.uk.xarixa.cloud.filesystem.core.utils;

import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * A {@link PathMatcher} capable of regex and glob style matches.
 */
public class DefaultPathMatcher implements PathMatcher {
	private final static Logger LOG = LoggerFactory.getLogger(DefaultPathMatcher.class);
	private final ThreadLocal<Matcher> patternMatcher;
	
	static ThreadLocal<Matcher> createLocalMatcher(String regex) {
		final Pattern compiledRegex = Pattern.compile(regex);

		return new ThreadLocal<Matcher>() {
			protected Matcher initialValue() {
				return compiledRegex.matcher("");
			}
		};
	}
	
	static String extractRegexFromGlobPattern(String globPattern, String pathSeparator) {
		StringBuilder regex = new StringBuilder();
		
		for (int i=0; i<globPattern.length(); i++) {
			char character = globPattern.charAt(i);

			if (character == '*') {
				// Check for extra * afterwards...
				int nextCharIndex = i + 1;
				if (nextCharIndex < globPattern.length() && globPattern.charAt(nextCharIndex) == '*') {
					// Glob: **
					regex.append(".*");
					i = nextCharIndex;
				} else {
					// Glob: *
					regex.append("[^" + pathSeparator + "]*");
				}
			} else if (character == '?') {
				// Glob: ?
				regex.append("[^" + pathSeparator + "]");
			} else if (character == '[') {
				// Glob: [ABCa-z]
				// Extract the rest of the expression
				int endExpression = i + 1 < globPattern.length() ? globPattern.indexOf(']', i + 1) : -1;
				if (endExpression == -1) {
					throw new IllegalArgumentException("Error at character " + i +
							", bracket expression started with no end character ']' (i.e. [a-z]) in glob expression: " + globPattern);
				}
				String expression = StringUtils.replaceOnce(regex.substring(i, endExpression), "\\!", "^");
				regex.append(expression);
			} else if (character == '\\') {
				// Glob: \c
				int nextCharIndex = i + 1;
				if (nextCharIndex >= globPattern.length()) {
					throw new IllegalArgumentException("Error at character " + i +
							", escaped character \\ used with no following character to escape in glob expression: " + globPattern);
				}
				regex.append(character).append(globPattern.charAt(nextCharIndex));
				i = nextCharIndex;
			} else if (character == '{') {
				// Glob: {java,class}
				// Extract the rest of the expression
				int endExpression = i + 1 < globPattern.length() ? globPattern.indexOf('}', i + 1) : -1;
				if (endExpression == -1) {
					throw new IllegalArgumentException("Error at character " + i +
							", group started with no end of grouping character '}' (i.e. {java,class}) in glob expression: " + globPattern);
				}
				String expression = StringUtils.replace(globPattern.substring(i + 1, endExpression), ",", "|");
				regex.append("(").append(expression).append(")");
				i = endExpression;
			} else {
				// Escape and append the character
				if (StringUtils.contains("$+|^.", character)) {
					regex.append("\\").append(character);
				} else {
					regex.append(character);
				}
			}
		}
		
		return regex.toString();
	}

	/**
	 * Constructs a default path matcher with the {@link CloudPath#DEFAULT_PATH_SEPARATOR default path separator}
	 * @param syntaxAndPattern
	 */
	public DefaultPathMatcher(String syntaxAndPattern) {
		this(syntaxAndPattern, CloudPath.DEFAULT_PATH_SEPARATOR);
	}

	/**
	 * Constructs a default path matcher with a given file system path separator
	 * @param syntaxAndPattern
	 * @param fileSystemPathSeparator	Path separator for this filesystem
	 */
	public DefaultPathMatcher(String syntaxAndPattern, String fileSystemPathSeparator) {
		String syntax = StringUtils.substringBefore(syntaxAndPattern, ":");
		String pattern = StringUtils.substringAfter(syntaxAndPattern, ":");
		String regex;

		if (StringUtils.equals(syntax, "glob")) {
			regex = extractRegexFromGlobPattern(pattern, fileSystemPathSeparator);
			LOG.debug("Created regex pattern '{}' from GLOB '{}'", regex, pattern);
		} else if (StringUtils.equals(syntax, "regex")) {
			regex = pattern;
		} else {
			throw new IllegalArgumentException("Unknown syntax '" + syntax + "' from syntax/pattern definition: " + syntaxAndPattern);
		}

		patternMatcher = createLocalMatcher(regex);
	}

	@Override
	public boolean matches(Path path) {
		return matches(path.toAbsolutePath().toString());
	}

	boolean matches(String path) {
		return patternMatcher.get().reset(path).matches();
	}

}
