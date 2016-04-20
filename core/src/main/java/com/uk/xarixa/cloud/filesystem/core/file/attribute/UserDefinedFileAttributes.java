package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class UserDefinedFileAttributes implements FileAttribute<Map<String,String>> {
	private final Map<String,String> userDefinedMap = new HashMap<>();
	
	public UserDefinedFileAttributes(Map<String,String> userDefinedMap) {
		this.userDefinedMap.putAll(userDefinedMap);
	}

	@Override
	public String name() {
		return "cloud:userDefinedMap";
	}

	@Override
	public Map<String,String> value() {
		return Collections.unmodifiableMap(userDefinedMap);
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}