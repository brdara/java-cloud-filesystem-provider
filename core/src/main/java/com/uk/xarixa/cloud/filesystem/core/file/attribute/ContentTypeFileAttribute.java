package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.FileAttribute;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.google.common.net.MediaType;

/**
 * Note that the {@link MediaType} may contain a character set, so it is possible to use the media type
 * with a character set rather than the {@link ContentEncodingFileAttribute} file attribute 
 */
public class ContentTypeFileAttribute implements FileAttribute<MediaType> {
	private final MediaType mediaType;
	
	public ContentTypeFileAttribute(MediaType mediaType) {
		this.mediaType = mediaType;
	}

	@Override
	public String name() {
		return "cloud:contentType";
	}

	@Override
	public MediaType value() {
		return mediaType;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}
}
