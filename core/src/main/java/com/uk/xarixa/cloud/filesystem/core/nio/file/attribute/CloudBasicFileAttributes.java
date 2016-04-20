package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.net.URI;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.domain.Location;
import org.jclouds.io.ContentMetadata;

import com.google.common.hash.HashCode;

public class CloudBasicFileAttributes implements BasicFileAttributes {
	private final FileTime lastModified;
	private final FileTime created;
	private final String key;
	private final StorageType storageType;
	private final Location physicalLocation;
	private final URI uri;
	private final Long size;
	private final String eTag;
	private final String contentDisposition;
	private final String contentEncoding;
	private final String contentLanguage;
	private final HashCode contentMD5;
	private final String contentType;
	private final FileTime expires;
	private final Map<String, String> userMetadata;
	
	public CloudBasicFileAttributes(BlobMetadata blobMetadata) {
		lastModified = FileTime.from(Optional.ofNullable(blobMetadata.getLastModified().getTime()).orElse(0L), TimeUnit.MILLISECONDS);
		created = blobMetadata.getCreationDate() == null ? lastModified : FileTime.from(blobMetadata.getCreationDate().getTime(), TimeUnit.MILLISECONDS);
		key = blobMetadata.getProviderId();
		storageType = blobMetadata.getType();
		physicalLocation = blobMetadata.getLocation();
		uri = blobMetadata.getUri();
		size = Optional.ofNullable(blobMetadata.getSize()).orElse(Long.valueOf(0));
		eTag = blobMetadata.getETag();
		userMetadata = blobMetadata.getUserMetadata();

		if (isRegularFile()) {
			ContentMetadata contentMetadata = blobMetadata.getContentMetadata();
			contentDisposition = contentMetadata.getContentDisposition();
			contentEncoding = contentMetadata.getContentEncoding();
			contentLanguage = contentMetadata.getContentLanguage();
			contentMD5 = contentMetadata.getContentMD5AsHashCode();
			contentType = contentMetadata.getContentType();
			expires = contentMetadata.getExpires() == null ? null :
				FileTime.from(contentMetadata.getExpires().getTime(), TimeUnit.MILLISECONDS);
		} else {
			contentDisposition = null;
			contentEncoding = null;
			contentLanguage = null;
			contentMD5 = null;
			contentType = null;
			expires = null;
		}
	}

	/**
	 * Placeholder indicates a directory
	 */
	public CloudBasicFileAttributes() {
		lastModified = FileTime.from(0L, TimeUnit.MILLISECONDS);
		created = FileTime.from(0L, TimeUnit.MILLISECONDS);
		key = null;
		storageType = StorageType.FOLDER;
		physicalLocation = null;
		uri = null;
		size = null;
		eTag = null;
		userMetadata = null;
		contentDisposition = null;
		contentEncoding = null;
		contentLanguage = null;
		contentMD5 = null;
		contentType = null;
		expires = null;
	}

	public CloudBasicFileAttributes(StorageMetadata meta) {
		lastModified = meta.getLastModified() == null ? FileTime.from(0L, TimeUnit.MILLISECONDS) :
			FileTime.from(meta.getLastModified().getTime(), TimeUnit.MILLISECONDS);
		created = meta.getCreationDate() == null ? lastModified : FileTime.from(meta.getCreationDate().getTime(), TimeUnit.MILLISECONDS);
		key = meta.getProviderId();
		storageType = meta.getType();
		physicalLocation = meta.getLocation();
		uri = meta.getUri();
		size = Optional.ofNullable(meta.getSize()).orElse(Long.valueOf(0));
		eTag = meta.getETag();
		userMetadata = meta.getUserMetadata();
		contentDisposition = null;
		contentEncoding = null;
		contentLanguage = null;
		contentMD5 = null;
		contentType = null;
		expires = null;
	}

	@Override
	public FileTime lastModifiedTime() {
		return lastModified;
	}

	@Override
	public FileTime lastAccessTime() {
		return lastModified;
	}

	@Override
	public FileTime creationTime() {
		return created;
	}

	@Override
	public boolean isRegularFile() {
		return StorageType.BLOB.equals(storageType);
	}

	@Override
	public boolean isDirectory() {
		return StorageType.RELATIVE_PATH.equals(storageType) || StorageType.FOLDER.equals(storageType);
	}

	/**
	 * Always returns false
	 */
	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public boolean isOther() {
		return !isRegularFile() && !isDirectory();
	}
	
	public boolean isContainer() {
		return StorageType.CONTAINER.equals(storageType);
	}

	@Override
	public long size() {
		return size;
	}

	/**
	 * @see BlobMetadata#getProviderId()
	 */
	@Override
	public Object fileKey() {
		return key;
	}
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	/**
	 * Gets the physical location for this content
	 * @return
	 */
	public Location getPhysicalLocation() {
		return physicalLocation;
	}

	/**
	 * Gets a URI for accessing this content
	 * @return
	 */
	public URI getUri() {
		return uri;
	}

	/**
	 * The ETag for this content
	 * @return
	 */
	public String getETag() {
		return eTag;
	}

	/**
	 * If {@link #isRegularFile()} returns the content disposition, null otherwise
	 * @return
	 */
	public String getContentDisposition() {
		return contentDisposition;
	}

	/**
	 * If {@link #isRegularFile()} returns the content encoding, null otherwise
	 * @return
	 */
	public String getContentEncoding() {
		return contentEncoding;
	}

	/**
	 * If {@link #isRegularFile()} returns the content language, null otherwise
	 * @return
	 */
	public String getContentLanguage() {
		return contentLanguage;
	}

	/**
	 * If {@link #isRegularFile()} returns the content MD5 as a {@link HashCode}, null otherwise
	 * @return
	 */
	public HashCode getContentMD5() {
		return contentMD5;
	}

	/**
	 * If {@link #isRegularFile()} returns the content type, null otherwise
	 * @return
	 */
	public String getContentType() {
		return contentType;
	}

	/**
	 * If {@link #isRegularFile()} returns the content expiry date, null otherwise
	 * @return
	 */
	public FileTime getContentExpires() {
		return expires;
	}

	public Map<String, String> getUserMetadata() {
		return userMetadata;
	}

	public StorageType getStorageType() {
		return storageType;
	}
}
