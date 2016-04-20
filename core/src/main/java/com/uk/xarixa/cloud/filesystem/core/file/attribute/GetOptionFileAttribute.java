package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.jclouds.blobstore.options.GetOptions;

public class GetOptionFileAttribute implements FileAttribute<GetOptions> {
	private final GetOptions getOptions;
	
	public GetOptionFileAttribute(GetOptions getOptions) {
		this.getOptions = getOptions;
	}

	@Override
	public String name() {
		return "cloud:getOption";
	}

	@Override
	public GetOptions value() {
		return getOptions;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
