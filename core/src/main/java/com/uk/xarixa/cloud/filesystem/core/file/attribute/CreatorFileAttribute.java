package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Represents a file creator as a file attribute
 */
public class CreatorFileAttribute <ID extends Object> implements FileAttribute<ID> {

	@Override
	public String name() {
		return "cloud:creator";
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	/**
	 * The owner ID
	 */
	@Override
	public ID value() {
		return null;
	}

}