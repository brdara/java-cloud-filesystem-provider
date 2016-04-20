package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class ContentDispositionFileAttribute implements FileAttribute<String> {
	private final String contentDisposition;
	
	public ContentDispositionFileAttribute(String contentDisposition) {
		this.contentDisposition = contentDisposition;
	}

	@Override
	public String name() {
		return "cloud:contentDisposition";
	}

	@Override
	public String value() {
		return contentDisposition;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
