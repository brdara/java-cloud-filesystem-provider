package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class CloudPermissionFileAttribute<P extends Object> implements FileAttribute<P> {
	private final P permissions;
	
	public CloudPermissionFileAttribute(P permissions) {
		this.permissions = permissions;
	}

	@Override
	public String name() {
		return "cloud:permission";
	}

	@Override
	public P value() {
		return permissions;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
