package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class ContentEncodingFileAttribute implements FileAttribute<String> {
	private final String contentEncoding;
	
	public ContentEncodingFileAttribute(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	@Override
	public String name() {
		return "cloud:contentEncoding";
	}

	@Override
	public String value() {
		return contentEncoding;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
