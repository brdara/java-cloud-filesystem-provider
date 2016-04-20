package com.uk.xarixa.cloud.filesystem.core.utils;

/**
 * Safe version of {@link Cloneable}
 */
public interface SafeCloneable<T extends Object> {

	T clone();

}
