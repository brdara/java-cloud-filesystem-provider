package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.jclouds.blobstore.BlobStoreContext;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.CloudFileChannel;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

/**
 * A cloud file system implementation is used by the {@link CloudFileSystemProviderDelegate} to delegate
 * the cloud interaction methods.
 * @see CloudHostConfiguration#getCloudFileSystemImplementation()
 */
public interface CloudFileSystemImplementation {

	/**
	 * Which method which was used to perform the cloud operation
	 */
	public static enum CloudMethod {
		/**
		 * Optimised cloud copy method was used
		 */
		CLOUD_OPTIMISED,
		/**
		 * A vendor-specific implementation was used
		 */
		VENDOR_SPECIFIC,
		/**
		 * A local filesystem fallback method was used, which usually involves copying the file to the local file system
		 * first from the cloud and then sync'ing it back up to the cloud
		 */
		LOCAL_FILESYSTEM_FALLBACK;
	}


	/**
	 * @see FileSystemProvider#newByteChannel(Path, Set, FileAttribute...)
	 */
	CloudFileChannel newByteChannel(BlobStoreContext context,
			CloudPath path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException;

	/**
	 * @see FileSystemProvider#newDirectoryStream(Path, Filter)
	 */
	CloudDirectoryStream newDirectoryStream(BlobStoreContext context,
			CloudPath dir, Filter<CloudPath> filter) throws IOException;

	/**
	 * A version of {@link #newDirectoryStream(BlobStoreContext, CloudPath, Filter)} which can recursively
	 * list all content beneath a directory when <em>isRecursive</em> is set to true. This is more
	 * efficient than invoking multiple calls to get subdirectories.
	 * @see FileSystemProvider#newDirectoryStream(Path, Filter)
	 */
	CloudDirectoryStream newDirectoryStream(BlobStoreContext context, CloudPath dir,
			Filter<CloudPath> filter, boolean isRecursive) throws IOException;

	/**
	 * @see FileSystemProvider#createDirectory(Path, FileAttribute...)
	 */
	void createDirectory(BlobStoreContext context,
			CloudPath dir, FileAttribute<?>... attrs) throws IOException;

	/**
	 * @see FileSystemProvider#delete(Path)
	 */
	void delete(BlobStoreContext context, CloudPath path, EnumSet<DeleteOption> options) throws IOException;

	/**
	 * @see CloudFileSystemProvider#delete(Iterable, DeleteOption...)
	 */
	void delete(BlobStoreContext context, Collection<CloudPath> paths, EnumSet<DeleteOption> options) throws IOException;

	/**
	 * @see FileSystemProvider#copy(Path, Path, CopyOption...)
	 */
	CloudMethod copy(BlobStoreContext context, CloudPath source, Path target, Set<CopyOption> options) throws IOException;

	/**
	 * @see FileSystemProvider#copy(Path, Path, CopyOption...)
	 */
	Map<CloudPath,CloudMethod> copy(BlobStoreContext context, Set<CloudPath> sources, Path target, Set<CopyOption> options) throws IOException;

	/**
	 * @see FileSystemProvider#move(Path, Path, CopyOption...)
	 */
	CloudMethod move(BlobStoreContext context, CloudPath source, Path target, Set<CopyOption> options) throws IOException;

	/**
	 * @see FileSystemProvider#checkAccess(Path, AccessMode...)
	 */
	void checkAccess(BlobStoreContext context,
			CloudPath path, Set<AclEntryPermission> checkPermissions) throws IOException;

	/**
	 * @see FileSystemProvider#getFileAttributeView(Path, Class, LinkOption...)
	 */
	<V extends FileAttributeView> V getFileAttributeView(BlobStoreContext blobStoreContext, Class<V> type, CloudPath cloudPath);

	/**
	 * @see FileSystemProvider#readAttributes(Path, Class, LinkOption...)
	 */
	<A extends BasicFileAttributes> A readAttributes(BlobStoreContext blobStoreContext, Class<A> type,
			CloudPath cloudPath) throws IOException;

	/**
	 * @see FileSystemProvider#readAttributes(Path, String, LinkOption...)
	 * @param path
	 * @param attributes
	 * @param options
	 * @return
	 */
	Map<String, Object> readAttributes(BlobStoreContext context,
			CloudPath path, String attributes) throws IOException;

	/**
	 * @throws IOException 
	 * @see FileSystemProvider#setAttribute(Path, String, Object, LinkOption...)
	 */
	void setAttribute(BlobStoreContext context,
			CloudPath path, String attribute, Object value) throws IOException;

}
