package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * A fast lookup map for {@link FileAttribute} types
 */
public class FileAttributeLookupMap {
	private final Map<Long,FileAttribute<?>> fileAttributes = new HashMap<>();

	@SuppressWarnings("unchecked")
	public FileAttributeLookupMap(FileAttribute<?>... fileAttributes) {
		this(fileAttributes != null ? Arrays.asList(fileAttributes) : Collections.EMPTY_SET);
	}

	@SuppressWarnings("unchecked")
	public FileAttributeLookupMap(Collection<? extends FileAttribute<?>> fileAttributes) {
		try {
			fileAttributes.forEach(a ->
				this.fileAttributes.put(
						classKeyBuilder(((Class<? extends FileAttribute<?>>)a.getClass()), a.value().getClass()),
						a));
		} catch (NullPointerException npe) {
			throw new IllegalArgumentException("File attribute values or entries are null in the file attributes list: " +
					fileAttributes);
		}
	}

	/**
	 * Gets the first file attribute of a specified type
	 * @param fileAttributeClass
	 * @return
	 * @see #getFileAttributesOfType(Class)
	 */
	public <T extends FileAttribute<?>> T getFirstFileAttributeOfType(Class<T> fileAttributeClass) {
		Collection<T> fileAttributesOfType = getFileAttributesOfType(fileAttributeClass);
		
		if (fileAttributesOfType.isEmpty()) {
			return null;
		}
		
		return fileAttributesOfType.iterator().next();
	}

	/**
	 * Gets the file attributes with the given {@link FileAttribute} class
	 * @param fileAttributeClass
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T extends FileAttribute<?>> Collection<T> getFileAttributesOfType(Class<T> fileAttributeClass) {
		// Lookup by shifting the key back and testing the values against the hash code
		long fileAttributeClassHashCode = fileAttributeClass.hashCode();
		return (Collection<T>)fileAttributes.entrySet()
	        .stream()
	        .filter(p -> p.getKey() >> 32 == fileAttributeClassHashCode)
	        .map(p -> p.getValue())
	        .collect(Collectors.toList());
	}

	/**
	 * Gets the file attribute with the given {@link FileAttribute} class and given {@link FileAttribute#value() return value class}
	 * @param fileAttributeClass
	 * @param fileAttributeValueClass
	 * @return
	 */
	public <T extends FileAttribute<?>> T getFileAttributeOfType(Class<T> fileAttributeClass, Class<?> fileAttributeValueClass) {
		FileAttribute<?> fileAttribute = fileAttributes.get(classKeyBuilder(fileAttributeClass, fileAttributeValueClass));
		if (fileAttribute != null) {
			return fileAttributeClass.cast(fileAttribute);
		}

		return null;
	}

	/**
	 * Creates a key from the class hash codes to be used in the map. This is a fast operation as it uses
	 * the fact that {@link Class#hashCode()} returns an integer value, so the long is formed by shifting
	 * the integer value of the first class to be the upper-most bits of the long and the lower part
	 * is composed of the hash code for the other class.
	 * @param class1
	 * @param fileAttributeValueClass
	 * @return
	 */
	public static Long classKeyBuilder(Class<? extends FileAttribute<?>> fileAttributeClass, Class<?> fileAttributeValueClass) {
		return (((long)fileAttributeClass.hashCode()) << 32) +
				(fileAttributeValueClass == null ? 0 : fileAttributeValueClass.hashCode());
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
