package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.jclouds.blobstore.options.PutOptions;

public class PutOptionFileAttribute implements FileAttribute<PutOptions> {
	private final PutOptions putOptions;
	
	public PutOptionFileAttribute(PutOptions putOptions) {
		this.putOptions = putOptions;
	}

	@Override
	public String name() {
		return "cloud:putOption";
	}

	@Override
	public PutOptions value() {
		return putOptions;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
