package com.uk.xarixa.cloud.filesystem.core.nio;

import org.jclouds.s3.S3Client;
import org.jclouds.s3.domain.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.file.attribute.CloudPermissionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.FileAttributeLookupMap;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.CloudFileChannel;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.CloudFileChannelTransport;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.DefaultCloudFileChannelTransport;

public class AwsCloudFileSystemImplementation extends DefaultCloudFileSystemImplementation {
	private final static Logger LOG = LoggerFactory.getLogger(AwsCloudFileSystemImplementation.class);

	private final static CloudFileChannelTransport TRANSPORT = new DefaultCloudFileChannelTransport() {
		
		@Override
		public void postSyncToCloud(CloudFileChannel cloudFileChannel, boolean writeMetadata) {
			// Other options only apply if we are writing metadata
			if (writeMetadata) {
				FileAttributeLookupMap lookupMap = cloudFileChannel.getCloudFileAttributes();
				@SuppressWarnings("unchecked")
				CloudPermissionFileAttribute<AccessControlList> aclAttribute =
						lookupMap.getFileAttributeOfType(CloudPermissionFileAttribute.class, AccessControlList.class);

				if (aclAttribute != null) {
					LOG.info("Setting AWS ACL attributes: path={}, owner={}, grants={}",
							cloudFileChannel.getPath(), aclAttribute.value().getOwner(), aclAttribute.value().getGrants());
					S3Client s3Client = cloudFileChannel.getContext().unwrapApi(S3Client.class);
					s3Client.putObjectACL(cloudFileChannel.getPath().getContainerName(),
							cloudFileChannel.getPath().getPathName(), aclAttribute.value());
				} else {
					// Run the super action
					LOG.debug("No ACL attibute set, falling back to generic method");
					super.postSyncToCloud(cloudFileChannel, writeMetadata);
				}
			} else {
				LOG.debug("No file post-sync action");
			}
		}

	};
	
	@Override
	protected CloudFileChannelTransport getCloudFileChannelTransport() {
		return TRANSPORT;
	}

}
