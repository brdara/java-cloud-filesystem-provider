package com.uk.xarixa.cloud.filesystem.core.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Optimizes regex pattern creation utilising a {@link ThreadLocal}.
 *
 * @author alexbrd
 */
public class OptimizedPatternMatcher {
        private ThreadLocal<Matcher> matcherLocal = new ThreadLocal<Matcher>();

        public OptimizedPatternMatcher(final String regex) {
                matcherLocal = new ThreadLocal<Matcher>() {
                        @Override
                        protected Matcher initialValue() {
                                return Pattern.compile(regex).matcher("");
                        }
                };
        }

        public OptimizedPatternMatcher(final String regex, final int flags) {
                matcherLocal = new ThreadLocal<Matcher>() {
                        @Override
                        protected Matcher initialValue() {
                                return Pattern.compile(regex, flags).matcher("");
                        }
                };
        }

        public Matcher getMatcher(String inputString) {
                return matcherLocal.get().reset(StringUtils.isNotBlank(inputString) ? inputString : "");
        }

}

