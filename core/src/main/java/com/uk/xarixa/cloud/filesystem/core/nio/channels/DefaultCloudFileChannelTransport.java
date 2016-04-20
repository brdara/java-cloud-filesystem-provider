package com.uk.xarixa.cloud.filesystem.core.nio.channels;

import java.nio.file.Path;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.FilePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.file.attribute.CloudPermissionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.FileAttributeLookupMap;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.GetOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.PutOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * A default implementation of {@link CloudFileChannelTransport}. This can be overriden to provide
 * specific implementations. The default implementation uses standard JClouds functionality.
 */
public class DefaultCloudFileChannelTransport implements CloudFileChannelTransport {
	private final static Logger LOG = LoggerFactory.getLogger(DefaultCloudFileChannelTransport.class);
	public final static DefaultCloudFileChannelTransport INSTANCE = new DefaultCloudFileChannelTransport();

	@Override
	public void preSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetadata) {
		LOG.debug("No file pre-sync action");
	}

	@Override
	public Payload createPayload(Path localFile) {
		return new FilePayload(localFile.toFile());
	}

	/**
	 * Looks for {@link CloudPermissionFileAttribute CloudFilePermissionFileAttribute&lt;BlobAccess&gt;}
	 * from {@link CloudFileChannel#getCloudFileAttributes()} and applies these generically using JClouds.
	 * More specific behaviours can be achieved by extending/re-implementing this method.
	 */
	@Override
	public void postSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetadata) {
		// Other options only apply if we are writing metadata
		if (writeMetadata) {
			FileAttributeLookupMap lookupMap = cloudFileChannel.getCloudFileAttributes();
			@SuppressWarnings("unchecked")
			CloudPermissionFileAttribute<BlobAccess> aclAttribute =
					lookupMap.getFileAttributeOfType(CloudPermissionFileAttribute.class, BlobAccess.class);

			if (aclAttribute != null) {
				CloudPath path = cloudFileChannel.getPath();
				LOG.info("Setting generic JClouds ACL attributes: path={}, access={}",
						path, aclAttribute.value().name());
				BlobStore blobStore = cloudFileChannel.getContext().getBlobStore();
				blobStore.setBlobAccess(path.getContainerName(), path.getPathName(), aclAttribute.value());
			} else {
				LOG.debug("No ACL attibute set, no file post-sync action");
			}
		} else {
			LOG.debug("No file post-sync action");
		}
	}

	/**
	 * Applies {@link PutOptionFileAttribute} if they exist to the {@link BlobStore#putBlob(String, Blob, PutOptions)} method.
	 * The <em>writeMetadata</em> flag is ignored.
	 */
	@Override
	public void storeBlob(BlobStoreContext blobStoreContext, String containerName, Blob blob, PutOptionFileAttribute putOption, boolean writeMetadata) {
		PutOptions putOptions = putOption == null ? PutOptions.NONE : putOption.value();
		blobStoreContext.getBlobStore().putBlob(containerName, blob, putOptions);
	}

	/**
	 * Applies the {@link GetOptionFileAttribute} if they exist to the {@link BlobStore#getBlob(String, String, GetOptions)} method.
	 */
	@Override
	public Blob getBlob(BlobStoreContext blobStoreContext, CloudPath path, GetOptionFileAttribute getOption) {
		GetOptions getOptions = getOption == null ? GetOptions.NONE : getOption.value();
		return blobStoreContext.getBlobStore().getBlob(path.getContainerName(), path.getPathName(), getOptions);
	}

}
