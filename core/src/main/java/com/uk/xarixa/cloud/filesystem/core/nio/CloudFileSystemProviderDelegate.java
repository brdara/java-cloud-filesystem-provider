package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.BlobStoreContext;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.factory.CloudHostProvider;
import com.uk.xarixa.cloud.filesystem.core.host.factory.JCloudsCloudHostProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudDirectoryStream;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.options.CloudCopyOption;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

/**
 * A base class to delegate {@link FileSystem} tasks to a specific provider of {@link CloudFileSystemImplementation}.
 * 
 * @see CloudHostConfiguration#getCloudFileSystemImplementation()
 */
public class CloudFileSystemProviderDelegate extends FileSystemProvider implements CloudFileSystemProvider {
	public static final String CLOUD_SCHEME = "cloud";
	protected static final CloudHostProvider cloudHostProvider = new JCloudsCloudHostProvider();


	@Override
	public String getScheme() {
		return CLOUD_SCHEME;
	}

	@Override
	public CloudFileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		if (!uri.getScheme().equals(CLOUD_SCHEME)) {
			throw new IllegalArgumentException("Invalid scheme in the file system URI, expected " + CLOUD_SCHEME +
					" but was " + uri.getScheme() + " from URI '" + uri.toString() + "'");
		}
		
		String hostname = uri.getHost();
		if (StringUtils.isBlank(hostname)) {
			throw new IllegalArgumentException("Invalid host in the file system URI, the host was empty in URI '" +
					uri.toString() + "', the host defines the cloud configuration to use such as cloud://example-aws-s3/bucket/path");
		}

		return cloudHostProvider.createCloudFileSystem(this, uri, env);
	}

	@Override
	public CloudFileSystem getFileSystem(URI uri) {
		CloudFileSystem cloudFileSystem = cloudHostProvider.getCloudFileSystem(uri);
		
		if (cloudFileSystem == null) {
			throw new FileSystemNotFoundException("Could not find the cloud file system for uri '" + uri.toString() + "'");
		}
		
		return cloudFileSystem;
	}

	@Override
	public CloudPath getPath(URI uri) {
		CloudFileSystem fileSystem = getFileSystem(uri);
		return new CloudPath(fileSystem, true, uri.getPath());
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		return newFileChannel(path, options, attrs);
	}
	
	@Override
	public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		CloudPath cloudPath = getCloudPath(path);
		CloudFileSystemImplementation cloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		return cloudFileSystemImplementation.newByteChannel(getBlobStoreContext(cloudPath), cloudPath, options, attrs);
	}
	
	/**
	 * Retrieves the {@link CloudFileSystemImplementation} from:
	 * <ol>
	 * <li>Invoking {@link CloudPath#getFileSystem()} to get the {@link CloudFileSystem}
	 * <li>Invoking {@link CloudFileSystem#getCloudHostConfiguration()} to get the {@link CloudHostConfiguration}
	 * <li>Invoking {@link CloudHostConfiguration#getCloudFileSystemImplementation()} to get the {@link CloudFileSystemImplementation}
	 * </ol>
	 * @param cloudPath
	 * @return
	 */
	protected CloudFileSystemImplementation getCloudFileSystemImplementation(CloudPath cloudPath) {
		CloudFileSystem fileSystem = cloudPath.getFileSystem();
		return fileSystem.getCloudHostConfiguration().getCloudFileSystemImplementation();
	}

	protected BlobStoreContext getBlobStoreContext(CloudPath cloudPath) {
		CloudFileSystem fileSystem = cloudPath.getFileSystem();
		return fileSystem.getBlobStoreContext();
	}

	private CloudPath getCloudPath(Path path) {
		if (!(path instanceof CloudPath)) {
			throw new IllegalArgumentException("Unrecognized path type " + path.getClass() +
					", it must be of type " + CloudPath.class);
		}
		
		 return (CloudPath)path;
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
		return newDirectoryStream(dir, filter, false);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter, boolean isRecursive)
			throws IOException {
		CloudPath cloudPath = getCloudPath(dir);
		CloudFileSystemImplementation cloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		CloudDirectoryStream newDirectoryStream =
				cloudFileSystemImplementation.newDirectoryStream(getBlobStoreContext(cloudPath), cloudPath,
						(Filter)filter, isRecursive);
		return (DirectoryStream)newDirectoryStream;
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		CloudPath cloudPath = getCloudPath(dir);
		CloudFileSystemImplementation cloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		cloudFileSystemImplementation.createDirectory(getBlobStoreContext(cloudPath), cloudPath, attrs);
	}

	@Override
	public void delete(Path path) throws IOException {
		CloudPath cloudPath = getCloudPath(path);
		doDelete(cloudPath);
	}
	
	private void doDelete(CloudPath cloudPath, DeleteOption... options) throws IOException {
		EnumSet<DeleteOption> deleteOptions = deleteOptionsToEnumSet(options);
		CloudFileSystemImplementation cloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		cloudFileSystemImplementation.delete(getBlobStoreContext(cloudPath), cloudPath, deleteOptions);
	}

	@Override
	public void delete(CloudPath path, DeleteOption... options) throws IOException {
		doDelete(path, options);
	}

	@Override
	public void delete(Set<CloudPath> paths, DeleteOption... options) throws IOException {
		delete(paths, deleteOptionsToEnumSet(options));
	}

	@Override
	public void delete(Set<CloudPath> paths, EnumSet<DeleteOption> deleteOptions) throws IOException {
		Map<CloudFileSystemImplementation, Collection<CloudPath>> pathsByImplementation =
				splitPathsByImplementation(paths);
		
		for (Map.Entry<CloudFileSystemImplementation, Collection<CloudPath>> implementationPaths : pathsByImplementation.entrySet()) {
			CloudFileSystemImplementation cloudFileSystemImplementation = implementationPaths.getKey();
			Collection<CloudPath> cloudPaths = implementationPaths.getValue();
			BlobStoreContext context = getBlobStoreContext(cloudPaths.stream().findFirst().get());
			cloudFileSystemImplementation.delete(context, cloudPaths, deleteOptions);
		}
	}

	EnumSet<DeleteOption> deleteOptionsToEnumSet(DeleteOption... options) {
		return Arrays.stream(options).collect(Collectors.toCollection(() -> EnumSet.noneOf(DeleteOption.class)));
	}

	/**
	 * Splits a set of paths into a map by their {@link CloudFileSystemImplementation}. This is used for the
	 * optimised {@link #delete(Set, EnumSet)}, {@link #delete(Set, DeleteOption...)} operations.
	 * @param paths
	 * @return
	 */
	Map<CloudFileSystemImplementation, Collection<CloudPath>> splitPathsByImplementation(Set<CloudPath> paths) {
		HashSetValuedHashMap<CloudFileSystemImplementation,CloudPath> map = new HashSetValuedHashMap<>();

		for (CloudPath path : paths) {
			CloudFileSystemImplementation cloudFileSystemImplementation = getCloudFileSystemImplementation(path);
			map.put(cloudFileSystemImplementation, path);
		}
		
		return map.asMap();
	}

	Set<CopyOption> copyOptionsToSet(CopyOption... options) {
		return Arrays.stream(options).collect(Collectors.toSet());
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		CloudPath sourceCloudPath = getCloudPath(source);
		Set<CopyOption> copyOptions = copyOptionsToSet(options);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(sourceCloudPath);
		sourceCloudFileSystemImplementation.copy(getBlobStoreContext(sourceCloudPath), sourceCloudPath, target, copyOptions);
	}
	
	@Override
	public void copy(Set<CloudPath> sources, Path target, CopyOption... options) throws IOException {
		Set<CopyOption> copyOptions = copyOptionsToSet(options);
		copy(sources, target, copyOptions);
	}
	
	@Override
	public void copy(Set<CloudPath> sources, Path target, Set<CopyOption> copyOptions) throws IOException {
		Set<CopyOption> internalCopyOptions = Sets.newHashSet(copyOptions);
		internalCopyOptions.add(CloudCopyOption.DONT_RETURN_COPY_METHOD);
		copyMultiplePaths(target, internalCopyOptions, sources);
	}

	private void copyMultiplePaths(Path target, Set<CopyOption> copyOptions, Set<CloudPath> copyPaths)
			throws IOException {
		if (!copyPaths.isEmpty()) {
			CloudPath firstDegradedCloudPath = copyPaths.stream().findFirst().get();
			CloudFileSystemImplementation cloudFileSystemImplementation = getCloudFileSystemImplementation(firstDegradedCloudPath);
			cloudFileSystemImplementation.copy(getBlobStoreContext(firstDegradedCloudPath),
					copyPaths, target, copyOptions);
		}
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		CloudPath sourceCloudPath = getCloudPath(source);
		Set<CopyOption> copyOptions = copyOptionsToSet(options);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(sourceCloudPath);
		sourceCloudFileSystemImplementation.move(getBlobStoreContext(sourceCloudPath), sourceCloudPath, target, copyOptions);
	}

	@Override
	public boolean isSameFile(Path path, Path other) throws IOException {
		return path.compareTo(other) == 0;
	}

	/**
	 * Always false, hidden files are not supported
	 * @param path
	 * @return
	 * @throws IOException
	 */
	@Override
	public boolean isHidden(Path path) throws IOException {
		return false;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		CloudPath cloudPath = getCloudPath(path);
		return cloudPath.getFileSystem().getFileStores().iterator().next();
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		CloudPath cloudPath = getCloudPath(path);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);

		// Work out permissions to check
		Set<AclEntryPermission> checkPermissions = EnumSet.noneOf(AclEntryPermission.class);
		Arrays.stream(modes).forEach(m ->
				{
					if (AccessMode.EXECUTE.equals(m)) {
						checkPermissions.add(AclEntryPermission.EXECUTE);
					} else if (AccessMode.READ.equals(m)) {
						checkPermissions.add(AclEntryPermission.READ_DATA);
					} else if (AccessMode.WRITE.equals(m)) {
						checkPermissions.add(AclEntryPermission.WRITE_DATA);
					}
				});

		sourceCloudFileSystemImplementation.checkAccess(getBlobStoreContext(cloudPath), cloudPath, checkPermissions);
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		CloudPath cloudPath = getCloudPath(path);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		return type.cast(sourceCloudFileSystemImplementation.getFileAttributeView(getBlobStoreContext(cloudPath), type, cloudPath));
	}

	/**
	 * Passes back a {@link CloudBasicFileAttributes} instance
	 * @see CloudFileSystemImplementation#readAttributes(BlobStoreContext, CloudPath)
	 */
	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
			throws IOException {
		if (!BasicFileAttributes.class.isAssignableFrom(type) && !CloudBasicFileAttributes.class.isAssignableFrom(type)) {
			throw new IllegalArgumentException("The class type " + type + "{" + type.hashCode() +
					"} must be either " +
					BasicFileAttributes.class + "{" + BasicFileAttributes.class.hashCode() +
					"} or " + CloudBasicFileAttributes.class + "{" + CloudBasicFileAttributes.class.hashCode() + "}");
		}

		CloudPath cloudPath = getCloudPath(path);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		return type.cast(sourceCloudFileSystemImplementation.readAttributes(getBlobStoreContext(cloudPath), type, cloudPath));
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		CloudPath cloudPath = getCloudPath(path);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		return sourceCloudFileSystemImplementation.readAttributes(getBlobStoreContext(cloudPath), cloudPath, attributes);
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		CloudPath cloudPath = getCloudPath(path);
		CloudFileSystemImplementation sourceCloudFileSystemImplementation = getCloudFileSystemImplementation(cloudPath);
		sourceCloudFileSystemImplementation.setAttribute(getBlobStoreContext(cloudPath), cloudPath, attribute, value);
	}

	@Override
	public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException {
		throw new UnsupportedOperationException("Cannot create a cloud filesystem from a file");
	}

	@Override
	public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
		return super.newInputStream(path, options);
	}

	@Override
	public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
		return super.newOutputStream(path, options);
	}

	@Override
	public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options,
			ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
		return super.newAsynchronousFileChannel(path, options, executor, attrs);
	}

	@Override
	public void createLink(Path link, Path existing) throws IOException {
		super.createLink(link, existing);
	}

	@Override
	public boolean deleteIfExists(Path path) throws IOException {
		return super.deleteIfExists(path);
	}

	@Override
	public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
		super.createSymbolicLink(link, target, attrs);
	}

	@Override
	public Path readSymbolicLink(Path link) throws IOException {
		return super.readSymbolicLink(link);
	}

}
