package com.uk.xarixa.cloud.filesystem.core.nio.channels;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobBuilder.PayloadBlobBuilder;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.MediaType;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentDispositionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentEncodingFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentLanguageFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentTypeFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.FileAttributeLookupMap;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.GetOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.PutOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.UserDefinedFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * For each open option this will:
 * <ul>
 * <li>{@link StandardOpenOption#APPEND} - Create a copy of the file on the local filesystem, append and then
 * write back to the store on close.
 * <li>{@link StandardOpenOption#CREATE} - Checks if the file or an existing lock for the file exists. A lock is created if
 * 											it does or it doesn't. File is created
 * 											locally and then sync'd up to the cloud file system on {@link #close()} or
 * 											{@link #force(boolean)}
 * <li>{@link StandardOpenOption#CREATE_NEW} - The same as for {@link StandardOpenOption#CREATE} except it will thrown an
 * 												exception if the file exists. The lock is still created.
 * <li>{@link StandardOpenOption#WRITE} - File is copied to local store and written to. The file must exist.
 * <li>{@link StandardOpenOption#DELETE_ON_CLOSE} - File is deleted on close
 * <li>{@link StandardOpenOption#READ} - File is copied to local filesystem and read. It is deleted from the local on close.
 * <li>{@link StandardOpenOption#SPARSE} - Not supported
 * <li>{@link StandardOpenOption#DSYNC} - File is copied to local filesystem. On write the file is completely written to the
 * 											cloud file system
 * <li>{@link StandardOpenOption#SYNC} - The same as {@link StandardOpenOption#DSYNC}
 * <li>{@link StandardOpenOption#TRUNCATE_EXISTING} - Effectively the same as {@link StandardOpenOption#CREATE}
 * </ul>
 * 
 */
public class CloudFileChannel extends FileChannel {
	private static final String DEFAULT_CONTENT_TYPE = MediaType.OCTET_STREAM.toString();
	private final static Logger LOG = LoggerFactory.getLogger(CloudFileChannel.class);
	private final BlobStoreContext context;
	private final CloudPath path;
	private final Path localPath;
	private final Set<? extends OpenOption> cloudFileOptions;
	private final List<FileAttribute<?>> cloudFileAttributes = new ArrayList<>();
	private final AtomicInteger syncCount = new AtomicInteger(0);
	private final boolean writeShouldSyncMetadata;
	private final boolean writeShouldSync;
	private final CloudFileChannelTransport transport;
	private final FileAttributeLookupMap fileAttributesLookupMap;
	private FileChannel channel;

	public CloudFileChannel(BlobStoreContext context, CloudPath path, Set<? extends OpenOption> options,
			FileAttribute<?>... attrs) throws IOException {
		this(context, path, DefaultCloudFileChannelTransport.INSTANCE, options, attrs);
	}

	public CloudFileChannel(BlobStoreContext context, CloudPath path, CloudFileChannelTransport transport,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		
		if (options.contains(StandardOpenOption.SPARSE)) {
			LOG.warn("Open option specifies a sparse file, this will be ignored as sparse files cannot be created in the cloud");
		}
		
		this.transport = transport;
		this.context = context;
		this.path = path;
		this.cloudFileOptions = Collections.unmodifiableSet(options);
		this.cloudFileAttributes.addAll(Arrays.asList(attrs));
		this.fileAttributesLookupMap = new FileAttributeLookupMap(attrs);
		this.writeShouldSyncMetadata = options.contains(StandardOpenOption.SYNC);
		this.writeShouldSync = options.contains(StandardOpenOption.DSYNC) || writeShouldSyncMetadata;
		
		// Create a temp file
		localPath = Files.createTempFile("cloud-temp-", path.getPathName().replaceAll("/", "_"));
		localPath.toFile().deleteOnExit();
		
		// Flag to indicate immediate removal
		boolean removeTempFile = true;

		try {
			boolean download = false;

			// Does this file exist?
			if (path.exists()) {
				if (options.contains(StandardOpenOption.CREATE_NEW)) {
					throw new FileAlreadyExistsException("Cannot create a new file with an existing path for '" +
							path + "'");
				}
		
				// Download the file and then open on FS if we aren't truncating it
				download = !options.contains(StandardOpenOption.TRUNCATE_EXISTING);
			} else {
				if (!options.contains(StandardOpenOption.CREATE) && !options.contains(StandardOpenOption.CREATE_NEW)) {
					throw new IllegalArgumentException("File '" + path +
							"' does not exist and create was not specified as an open option");
				}
			}

			// Copy the blob from S3 to local if required
			if (download) {
				LOG.debug("Downloading '{}' to '{}'", path, localPath);
				Blob blob = transport.getBlob(context, path, getGetOption());
				Files.copy(blob.getPayload().openStream(), localPath, StandardCopyOption.REPLACE_EXISTING);
				LOG.debug("Completed downloading '{}' to '{}'", path, localPath);
			}

			EnumSet<StandardOpenOption> openOptions =
					EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE);
			if (options.contains(StandardOpenOption.READ)) {
				openOptions.add(StandardOpenOption.READ);
			}
			LOG.debug("Creating local file '{}' with open options {}", localPath, openOptions);
			channel = FileChannel.open(localPath, openOptions);

			// Move to last position in the file
			if (options.contains(StandardOpenOption.APPEND)) {
				channel.position(channel.size());
			}

			removeTempFile = false;
			LOG.debug("Created local file '{}' OK", localPath);
		} finally {
			if (removeTempFile) {
				LOG.debug("Removing temp file '{}'", localPath);
				Files.delete(localPath);
			}
		}
	}

	@Override
	public FileLock lock(long position, long size, boolean shared) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public FileLock tryLock(long position, long size, boolean shared) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void implCloseChannel() throws IOException {
		// First close the underlying file to close the channel, flush everything
		channel.close();
		LOG.debug("Closed local file '{}'", localPath);
		
		try {
			syncToBlobStore(true);
		} finally {
			// TODO: Allow options to not delete the local path? Retries for re-sync?
			Files.delete(localPath);
		}
	}
	
	/**
	 * Synchronizes the object to the BLOB store. The {@link CloudFileChannelTransport} is used here
	 * to construct the payload, and run a pre and post action.
	 * @param writeMetaData 	Whether to also write metadata or not
	 * @throws IOException
	 */
	protected void syncToBlobStore(boolean writeMetaData) throws IOException {
		// Should we write?
		if (cloudFileOptions.contains(StandardOpenOption.WRITE) ||
				cloudFileOptions.contains(StandardOpenOption.APPEND) || writeShouldSync) {
			syncCount.incrementAndGet();
			LOG.debug("Executing pre-sync interceptor action {} for local file store '{}' to cloud path '{}'",
					transport.getClass().getName(), localPath, path);
			transport.preSyncToCloud(this, writeMetaData);
			LOG.debug("Executed pre-sync interceptor action {} for local file store '{}' to cloud path '{}' OK",
					transport.getClass().getName(), localPath, path);
			LOG.debug("Synchronizing from local file store '{}' to cloud path '{}'", localPath, path);
			File pathFile = localPath.toFile();
			
			// Sync to blob store
			Payload payload = transport.createPayload(localPath);
			try {
				// Read the file content from the channel so far
				BlobStore blobStore = context.getBlobStore();
				BlobBuilder blobBuilder = buildPayload(pathFile, payload, blobStore);
				transport.storeBlob(context, path.getContainerName(), blobBuilder.build(), getPutOption(), writeMetaData);
			} finally {
				payload.close();
			}
	
			LOG.info("Synchronized from local file store '{}' to cloud path '{}' OK", localPath, path);
			LOG.debug("Executing post-sync interceptor action {} for local file store '{}' to cloud path '{}'",
					transport.getClass().getName(), localPath, path);
			transport.postSyncToCloud(this, writeMetaData);
			LOG.debug("Executed post-sync interceptor action {} for local file store '{}' to cloud path '{}' OK",
					transport.getClass().getName(), localPath, path);
		}
	}

	protected PayloadBlobBuilder buildPayload(File pathFile, Payload payload, BlobStore blobStore) {
		PayloadBlobBuilder blobBuilder = blobStore.blobBuilder(path.getPathName())
			    .payload(payload);

		// Set the media type
		ContentTypeFileAttribute mediaType =
				fileAttributesLookupMap.getFileAttributeOfType(ContentTypeFileAttribute.class, MediaType.class);
		blobBuilder.contentLength(pathFile.length())
			    .contentType(mediaType == null ? DEFAULT_CONTENT_TYPE : mediaType.value().withoutParameters().toString());

		// Set the content disposition
		ContentDispositionFileAttribute contentDisposition =
				fileAttributesLookupMap.getFileAttributeOfType(ContentDispositionFileAttribute.class, String.class);
		if (contentDisposition != null) {
			blobBuilder.contentDisposition(contentDisposition.value());
		}

		// Set the content encoding
		ContentEncodingFileAttribute contentEncoding =
				fileAttributesLookupMap.getFileAttributeOfType(ContentEncodingFileAttribute.class, String.class);
		if (contentEncoding != null) {
			blobBuilder.contentEncoding(contentEncoding.value());
		} else if (mediaType != null && mediaType.value().charset().isPresent()) {
			blobBuilder.contentEncoding(mediaType.value().charset().get().toString());
		}

		// Set the content encoding
		ContentLanguageFileAttribute contentLanguage =
				fileAttributesLookupMap.getFileAttributeOfType(ContentLanguageFileAttribute.class, String.class);
		if (contentLanguage != null) {
			blobBuilder.contentLanguage(contentLanguage.value());
		}
		
		// User defined attributes
		Collection<UserDefinedFileAttributes> userDefinedAttributes =
				fileAttributesLookupMap.getFileAttributesOfType(UserDefinedFileAttributes.class);
		if (userDefinedAttributes != null && !userDefinedAttributes.isEmpty()) {
			UserDefinedFileAttributes userDefinedFileAttributes = userDefinedAttributes.stream().findFirst().get();
			blobBuilder.userMetadata(userDefinedFileAttributes.value());
		}

		return blobBuilder;
	}

	/**
	 * Checks if should also sync on a write
	 * @throws IOException 
	 */
	private void checkWriteToSync() throws IOException {
		if (writeShouldSync) {
			syncToBlobStore(writeShouldSyncMetadata);
		}
	}

	public int hashCode() {
		return channel.hashCode();
	}

	public boolean equals(Object obj) {
		return channel.equals(obj);
	}

	public String toString() {
		return channel.toString();
	}

	public int read(ByteBuffer dst) throws IOException {
		return channel.read(dst);
	}

	public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
		return channel.read(dsts, offset, length);
	}

	public int write(ByteBuffer src) throws IOException {
		int ret = channel.write(src);
		checkWriteToSync();
		return ret;
	}

	public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
		long ret = channel.write(srcs, offset, length);
		checkWriteToSync();
		return ret;
	}

	public long position() throws IOException {
		return channel.position();
	}

	public FileChannel position(long newPosition) throws IOException {
		return channel.position(newPosition);
	}

	public long size() throws IOException {
		return channel.size();
	}

	public FileChannel truncate(long size) throws IOException {
		channel = channel.truncate(size);
		checkWriteToSync();
		return this;
	}

	public void force(boolean metaData) throws IOException {
		channel.force(metaData);
		syncToBlobStore(metaData);
	}

	public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
		long ret = channel.transferTo(position, count, target);
		checkWriteToSync();
		return ret;
	}

	public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
		return channel.transferFrom(src, position, count);
	}

	public int read(ByteBuffer dst, long position) throws IOException {
		return channel.read(dst, position);
	}

	public int write(ByteBuffer src, long position) throws IOException {
		int ret = channel.write(src, position);
		checkWriteToSync();
		return ret;
	}

	/**
	 * LIMITATION: This does not work for {@link StandardOpenOption#SYNC} or {@link StandardOpenOption#DSYNC}
	 */
	public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
		return channel.map(mode, position, size);
	}

	public FileAttributeLookupMap getCloudFileAttributes() {
		return fileAttributesLookupMap;
	}

	public BlobStoreContext getContext() {
		return context;
	}

	public CloudPath getPath() {
		return path;
	}

	/**
	 * Gets the number of times that a sync was invoked
	 * @see #syncToBlobStore()
	 * @return
	 */
	public AtomicInteger getSyncCount() {
		return syncCount;
	}

	/**
	 * The {@link OpenOption} set which was used to create this channel.
	 * @return
	 */
	public Set<? extends OpenOption> getCloudFileOptions() {
		return cloudFileOptions;
	}

	public PutOptionFileAttribute getPutOption() {
		return fileAttributesLookupMap.getFirstFileAttributeOfType(PutOptionFileAttribute.class);
	}

	public GetOptionFileAttribute getGetOption() {
		return fileAttributesLookupMap.getFirstFileAttributeOfType(GetOptionFileAttribute.class);
	}

	public CloudFileChannelTransport getTransport() {
		return transport;
	}

}
