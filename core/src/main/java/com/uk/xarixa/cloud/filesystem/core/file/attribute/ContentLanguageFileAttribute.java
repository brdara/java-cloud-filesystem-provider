package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class ContentLanguageFileAttribute implements FileAttribute<String> {
	private final String contentLanguage;
	
	public ContentLanguageFileAttribute(String contentLanguage) {
		this.contentLanguage = contentLanguage;
	}

	@Override
	public String name() {
		return "cloud:contentLanguage";
	}

	@Override
	public String value() {
		return contentLanguage;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}