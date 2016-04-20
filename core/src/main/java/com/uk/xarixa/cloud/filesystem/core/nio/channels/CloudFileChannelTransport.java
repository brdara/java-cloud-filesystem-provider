package com.uk.xarixa.cloud.filesystem.core.nio.channels;

import java.nio.file.Path;

import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;

import com.uk.xarixa.cloud.filesystem.core.file.attribute.GetOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.PutOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * An interceptor for {@link CloudFileChannel} read/write operations.
 */
public interface CloudFileChannelTransport {

	/**
	 * Invoked before every sync is actioned in {@link CloudFileChannel#syncToBlobStore(boolean)}
	 * @param cloudFileChannel
	 * @param writeMetaData
	 */
	void preSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetaData);
	
	/**
	 * Creates a payload for the given file in the local filesystem which is to be copied in the
	 * {@link CloudFileChannel#syncToBlobStore(boolean)} action
	 * @param localFile
	 * @return
	 */
	Payload createPayload(Path localFile);

	/**
	 * Invoked by the {@link CloudFileChannel#syncToBlobStore(boolean)} action to store a BLOB
	 * @param containerName	The container 
	 * @param blob The BLOB to store
	 * @param cloudFileAttributes Attributes
	 */
	void storeBlob(BlobStoreContext blobStoreContext, String containerName, Blob blob, PutOptionFileAttribute putOption, boolean writeMetadata);

	/**
	 * Invoked after every sync is actioned to {@link CloudFileChannel#syncToBlobStore(boolean)}
	 * @param cloudFileChannel
	 * @param writeMetaData
	 */
	void postSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetaData);

	/**
	 * Retrieves the BLOB from the store
	 * @param blobStoreContext
	 * @param path
	 * @param fileAttributes
	 * @return
	 */
	Blob getBlob(BlobStoreContext blobStoreContext, CloudPath path, GetOptionFileAttribute getOption);

}
