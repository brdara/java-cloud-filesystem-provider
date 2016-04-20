package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.ContentMetadataBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.CloudFileChannel;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.CloudFileChannelTransport;
import com.uk.xarixa.cloud.filesystem.core.nio.channels.DefaultCloudFileChannelTransport;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.PublicPrivateCloudPermissionsPrincipal;
import com.uk.xarixa.cloud.filesystem.core.nio.options.CloudCopyOption;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;

/**
 * <p>
 * A default implementation of {@link CloudFileSystemImplementation}. This provides implementations for all of the
 * encapsulated methods
 * which will work cross-cloud using the standard JClouds libraries.
 * </p>
 * <p>
 * However, there may be more optimal ways of performing
 * some of the operations (such as multiple file delete) which can be optimised per cloud using a cloud specific
 * implementation. In these cases a subclass can be created which overrides these methods and unwraps the
 * JClouds API, for example an AWS-specific JClouds API client would be unwrapped like this:
 * <pre>
 * S3Client s3Client = context.unwrapApi(S3Client.class);
 * </pre>
 * </p>
 * <p>
 * A {@link CloudHostSecurityManager} if available is used in all methods to provide security checks around
 * file access through the {@link #checkAccess(BlobStoreContext, CloudPath, Set)} method.
 * </p>
 */
public class DefaultCloudFileSystemImplementation implements CloudFileSystemImplementation {
	public static final String ACL_SET_ATTRIBUTE = "aclSet";
	private static final Logger LOG = LoggerFactory.getLogger(DefaultCloudFileSystemImplementation.class);
	private static final Set<AclEntryPermission> CREATE_NEW_FILE_PERMS = EnumSet.of(AclEntryPermission.ADD_FILE);
	private static final Set<AclEntryPermission> NEW_DIRECTORY_STREAM_PERMS = EnumSet.of(AclEntryPermission.LIST_DIRECTORY);
	private static final Set<AclEntryPermission> CREATE_DIRECTORY_STREAM_PERMS = EnumSet.of(AclEntryPermission.ADD_SUBDIRECTORY);
	private static final Set<AclEntryPermission> DELETE_FILE_STREAM_PERMS = EnumSet.of(AclEntryPermission.DELETE);
	private static final Set<AclEntryPermission> DELETE_DIRECTORY_STREAM_PERMS =
			EnumSet.of(AclEntryPermission.DELETE, AclEntryPermission.DELETE_CHILD);
	private static final Set<AclEntryPermission> COPY_FILE_SOURCE_PERMS =
			EnumSet.of(AclEntryPermission.READ_DATA, AclEntryPermission.READ_ACL);
	private static final Set<AclEntryPermission> COPY_FILE_SOURCE_WITH_ATTRIBUTES_PERMS =
			EnumSet.of(AclEntryPermission.READ_ATTRIBUTES, COPY_FILE_SOURCE_PERMS.toArray(new AclEntryPermission[0]));
	private static final Set<AclEntryPermission> COPY_FILE_TARGET_PERMS =
			EnumSet.of(AclEntryPermission.WRITE_DATA, AclEntryPermission.WRITE_ACL);
	private static final Set<AclEntryPermission> COPY_FILE_TARGET_WITH_ATTRIBUTES_PERMS =
			EnumSet.of(AclEntryPermission.WRITE_ATTRIBUTES, COPY_FILE_TARGET_PERMS.toArray(new AclEntryPermission[0]));
	private static final Set<AclEntryPermission> COPY_FILE_TARGET_PARENT_PERMS =
			EnumSet.of(AclEntryPermission.ADD_FILE);
	private static final Set<AclEntryPermission> COPY_DIR_SOURCE_PERMS =
			EnumSet.of(AclEntryPermission.LIST_DIRECTORY, AclEntryPermission.READ_DATA, AclEntryPermission.READ_ACL);
	private static final Set<AclEntryPermission> COPY_DIR_SOURCE_WITH_ATTRIBUTES_PERMS =
			EnumSet.of(AclEntryPermission.READ_ATTRIBUTES, COPY_DIR_SOURCE_PERMS.toArray(new AclEntryPermission[0]));
	private static final Set<AclEntryPermission> COPY_DIR_TARGET_PERMS =
			EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY, AclEntryPermission.WRITE_DATA, AclEntryPermission.WRITE_ACL);
	private static final Set<AclEntryPermission> COPY_DIR_TARGET_WITH_ATTRIBUTES_PERMS =
			EnumSet.of(AclEntryPermission.WRITE_ATTRIBUTES, COPY_DIR_TARGET_PERMS.toArray(new AclEntryPermission[0]));
	private static final Set<AclEntryPermission> COPY_DIR_TARGET_PARENT_PERMS =
			EnumSet.of(AclEntryPermission.ADD_FILE, AclEntryPermission.ADD_SUBDIRECTORY);

	/**
	 * File access is checked using {@link #checkAccess(BlobStoreContext, CloudPath, Set)}
	 * always with {@link AclEntryPermission#WRITE_DATA} and {@link AclEntryPermission#ADD_FILE},
	 * and optionally with {@link AclEntryPermission#APPEND_DATA} if <em>options</em> contains
	 * {@link StandardOpenOption#APPEND}.
	 * @see	CloudFileChannel
	 */
	@Override
	public CloudFileChannel newByteChannel(BlobStoreContext context, CloudPath path,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		EnumSet<AclEntryPermission> channelPerms = EnumSet.noneOf(AclEntryPermission.class);
		options.forEach(o -> {
			AclEntryPermission aclPerm = openOptionToAclEntryPermission(o);
			if (aclPerm != null) {
				channelPerms.add(aclPerm);
			}
		});

		// Check the parent path for file add
		if (channelPerms.remove(AclEntryPermission.ADD_FILE)) {
			checkAccess(context, path.getParent(), CREATE_NEW_FILE_PERMS);
		}

		// Check file access if the file exists
		if (path.exists()) {
			checkAccess(context, path, channelPerms);
		}
		
		// Create the channel
		return new CloudFileChannel(context, path, getCloudFileChannelTransport(), options, attrs);
	}

	/**
	 * Transforms a {@link StandardOpenOption} into an {@link AclEntryPermission}. Other
	 * {@link OpenOption} types are ignored.
	 * @param o
	 * @return The option as an ACL permission or null if this is not applicable.
	 */
	protected AclEntryPermission openOptionToAclEntryPermission(OpenOption o) {
		if (o instanceof StandardOpenOption) {
			switch ((StandardOpenOption)o) {
				case APPEND: return AclEntryPermission.APPEND_DATA;
				case CREATE: return AclEntryPermission.ADD_FILE;
				case CREATE_NEW: return AclEntryPermission.ADD_FILE;
				case DELETE_ON_CLOSE: return AclEntryPermission.DELETE;
				case READ: return AclEntryPermission.READ_DATA;
				case TRUNCATE_EXISTING: return AclEntryPermission.APPEND_DATA;
				case WRITE: return AclEntryPermission.WRITE_DATA;
				default: return null;
			}
		}

		return null;
	}

	/**
	 * The {@link CloudFileChannelTransport} to use for all requests, defaults to
	 * {@link DefaultCloudFileChannelTransport#INSTANCE}
	 * @return
	 */
	protected CloudFileChannelTransport getCloudFileChannelTransport() {
		return DefaultCloudFileChannelTransport.INSTANCE;
	}

	/**
	 * Invokes {@link #newDirectoryStream(BlobStoreContext, CloudPath, Filter) newDirectoryStream(context, dir, filter, false}
	 */
	@Override
	public CloudDirectoryStream newDirectoryStream(BlobStoreContext context, CloudPath dir,
			Filter<CloudPath> filter) throws IOException {
		return newDirectoryStream(context, dir, filter, false);
	}

	/**
	 * Directory access is checked using {@link #checkAccess(BlobStoreContext, CloudPath, Set)}
	 * with {@link AclEntryPermission#LIST_DIRECTORY}.
	 */
	@Override
	public CloudDirectoryStream newDirectoryStream(BlobStoreContext context, CloudPath dir,
			Filter<CloudPath> filter, boolean isRecursive) throws IOException {
		checkAccess(context, dir, NEW_DIRECTORY_STREAM_PERMS);
		CloudPath dirPath = dir;
		boolean isContainer = dirPath.getRoot() == null;
		
		// Search in a container if root is null, otherwise check to find a directory
		if (!isContainer) {

			// Attempt to read attributes
			try {
				CloudBasicFileAttributes attributes = readAttributes(context, CloudBasicFileAttributes.class, dir);
				
				// Look at the parent if this is a file
				if (!attributes.isDirectory()) {
					dirPath = dir.getParent();
				}
			} catch (FileNotFoundException e) {
				LOG.warn("Cloud file path '{}' does not exist, will assume that this is a directory", dir);
			}

		}

		return new CloudDirectoryStream(dirPath, isContainer, isRecursive, filter);
	}

	/**
	 * Directory access is checked using {@link #checkAccess(BlobStoreContext, CloudPath, Set)}
	 * with {@link AclEntryPermission#ADD_SUBDIRECTORY}.
	 */
	@Override
	public void createDirectory(BlobStoreContext context, CloudPath dir, FileAttribute<?>... attrs) throws IOException {
		if (dir.exists()) {
			throw new FileAlreadyExistsException("The directory '" + dir.toAbsolutePath().toString() +
					"' cannot be created as it already exists");
		}
		
		// Check the directory has a parent
		CloudPath parent = dir.getParent();
		if (!parent.exists()) {
			throw new IOException("Could not create directory '" + dir.toAbsolutePath().toString() +
					"', the parent directory does not exist");
		}
		
		checkAccess(context, parent, CREATE_DIRECTORY_STREAM_PERMS);

		// File attributes don't apply here, directories are just placeholders with no access
		context.getBlobStore().createDirectory(dir.getContainerName(), dir.getPathName());
		LOG.debug("Created directory {}", dir);
	}

	/**
	 * Delete access is checked using {@link #checkAccess(BlobStoreContext, CloudPath, Set)}
	 * with {@link AclEntryPermission#DELETE}.
	 */
	@Override
	public void delete(BlobStoreContext context, CloudPath path, EnumSet<DeleteOption> options) throws IOException {

		// A container
		if (path.getRoot() == null) {
			checkAccess(context, path, DELETE_DIRECTORY_STREAM_PERMS);
			LOG.debug("Deleting Container '{}'...", path);
			context.getBlobStore().deleteContainer(path.getContainerName());
			LOG.debug("Deleted Container '{}' OK", path);
			return;
		}

		// Read the file/dir attributes
		CloudBasicFileAttributes readAttributes = readAttributes(context, CloudBasicFileAttributes.class, path);

		if (readAttributes.isDirectory()) {
			checkAccess(context, path, DELETE_DIRECTORY_STREAM_PERMS);

			// Delete all subpaths using the recursive directory listing
			Iterator<CloudPath> recursiveIterator = newDirectoryStream(context, path, null).iterator();

			if (recursiveIterator.hasNext() && !options.contains(DeleteOption.RECURSIVE)) {
				throw new DirectoryNotEmptyException("Directory '" + path.toAbsolutePath() +
						"' is not empty and the recursive delete option has not been specified");
			}

			LOG.debug("Deleting directory '{}' and all content recursively...", path);
			while (recursiveIterator.hasNext()) {
				delete(context, recursiveIterator.next(), options);
			}

			// Delete this path
			context.getBlobStore().deleteDirectory(path.getContainerName(), path.getPathName());
			LOG.debug("Deleted directory '{}' and all content recursively OK", path);
		} else if (readAttributes.isRegularFile()) {
			checkAccess(context, path, DELETE_FILE_STREAM_PERMS);
			LOG.debug("Deleting BLOB file '{}'...", path);
			context.getBlobStore().removeBlob(path.getContainerName(), path.getPathName());
			LOG.debug("Deleted BLOB file '{}' OK", path);
		} else if (readAttributes.isContainer()) {
			checkAccess(context, path, DELETE_DIRECTORY_STREAM_PERMS);
			LOG.debug("Deleting Container '{}'...", path);
			context.getBlobStore().deleteContainer(path.getContainerName());
			LOG.debug("Deleted Container '{}' OK", path);
		} else {
			throw new IllegalArgumentException("Cannot delete this path '" + path.toString() +
					"' it is not a directory, file or container");
		}
	}

	/**
	 * A non-optimised delete method using vanilla JClouds functionality which invokes
	 * {@link #delete(BlobStoreContext, CloudPath, EnumSet)} for each entry.
	 * Implementors can override this method to provide vendor-specific operations
	 * where applicable.
	 * 
	 * @param context
	 * @param paths
	 * @param options
	 * @throws IOException
	 */
	@Override
	public void delete(BlobStoreContext context, Collection<CloudPath> paths, EnumSet<DeleteOption> options)
			throws IOException {
		for (CloudPath path : paths) {
			try {
				delete(context, path, options);
			} catch (IOException e) {
				if (options.contains(CloudCopyOption.FAIL_SILENTLY)) {
					LOG.warn("Delete failed for {}, slient failure specified, continuing", path, e);
				} else {
					throw e;
				}
			}
		}
	}

	@Override
	public Map<CloudPath,CloudMethod> copy(BlobStoreContext context, Set<CloudPath> sources, Path target, Set<CopyOption> options)
			throws IOException {
		boolean copyMethodReturns = !options.contains(CloudCopyOption.DONT_RETURN_COPY_METHOD);
		Map<CloudPath,CloudMethod> copyMethodsMap = copyMethodReturns ? new HashMap<>() : null;

		for (CloudPath source : sources) {
			try {
				copy(context, source, target, options);
			} catch (IOException e) {
				if (options.contains(CloudCopyOption.FAIL_SILENTLY)) {
					LOG.warn("Copy from {} to {} failed, slient failure specified, continuing", source, target, e);
				} else {
					throw e;
				}
			}
		}

		return copyMethodsMap;
	}

	/**
	 * Copy access is checked using {@link #checkAccess(BlobStoreContext, CloudPath, Set)}
	 * with for a source which is a file:
	 * <ul>
	 * <li>On the source path: {@link AclEntryPermission#READ_DATA}, {@link AclEntryPermission#READ_ACL} and
	 * optionally if the options contains {@link StandardCopyOption#COPY_ATTRIBUTES}, {@link AclEntryPermission#READ_ATTRIBUTES}
	 * <li>On the target path, if it exists: {@link AclEntryPermission#WRITE_DATA}, {@link AclEntryPermission#WRITE_ACL} and
	 * optionally if the options contains {@link StandardCopyOption#COPY_ATTRIBUTES}, {@link AclEntryPermission#WRITE_ATTRIBUTES}
	 * <li>On the target's parent path: {@link AclEntryPermission#ADD_FILE}
	 * </ul>
	 * And for a source which is a directory:
	 * <ul>
	 * <li>On the source path: {@link AclEntryPermission#LIST_DIRECTORY},
	 * {@link AclEntryPermission#READ_DATA}, {@link AclEntryPermission#READ_ACL} and
	 * optionally if the options contains {@link StandardCopyOption#COPY_ATTRIBUTES}, {@link AclEntryPermission#READ_ATTRIBUTES}
	 * <li>On the target path: {@link AclEntryPermission#ADD_FILE}, {@link AclEntryPermission#ADD_SUBDIRECTORY},
	 * {@link AclEntryPermission#WRITE_DATA}, {@link AclEntryPermission#WRITE_ACL} and
	 * optionally if the options contains {@link StandardCopyOption#COPY_ATTRIBUTES}, {@link AclEntryPermission#WRITE_ATTRIBUTES}
	 * <li>On the target's parent path: {@link AclEntryPermission#ADD_FILE}, {@link AclEntryPermission#ADD_SUBDIRECTORY}
	 * </ul>
	 * ACL's will not be modified to include this user as an owner of any of the copied files, the ACL's are copied as-is
	 * using {@link #setAttribute(BlobStoreContext, CloudPath, String, Object) setAttribute(context, targetPath, ACL_SET_ATTRIBUTE, aclSet)}
	 * for each copied path.
	 * 
	 * @param context
	 * @param source
	 * @param target
	 * @param options
	 * @return
	 * @throws IOException
	 */
	@Override
	public CloudMethod copy(BlobStoreContext context, CloudPath source, Path target, Set<CopyOption> options) throws IOException {
		CloudMethod method = determineCloudMethodForCopyOrMoveOperations("copy", source, target, options);
		boolean fallbackToLocalFileSystemCopy = true;

		// Try cloud optimised method
		if (CloudMethod.CLOUD_OPTIMISED.equals(method)) {
			fallbackToLocalFileSystemCopy = !copyUsingOptimisedCopy(context, source, (CloudPath)target, options);
		}

		// Use fallback if required
		if (fallbackToLocalFileSystemCopy) {
			copyUsingLocalFilesystem(context, source, target, options);
			return CloudMethod.LOCAL_FILESYSTEM_FALLBACK;
		}
		
		return CloudMethod.CLOUD_OPTIMISED;
	}
	
	protected CloudMethod determineCloudMethodForCopyOrMoveOperations(
			String operationName, CloudPath source, Path target, Set<CopyOption> options) throws IOException {
		// Don't copy a file to itself
		if (source.equals(target)) {
			throw new FileAlreadyExistsException("Cannot " + operationName + " a path to itself from '" + source.toAbsolutePath() +
					"' -> '" + target.toAbsolutePath() + "'");
		}

		// Check if target exists and we are allowed to copy
		if (Files.exists(target) && !options.contains(StandardCopyOption.REPLACE_EXISTING)) {
			throw new FileAlreadyExistsException("Cannot copy from " + source.toAbsolutePath() +
					" to " + target.toAbsolutePath() + ", the file already exists and file replace was not specified");
		}

		// Check if we can try a JClouds copy or we have to use the filesystem
		if (target instanceof CloudPath) {
			// Work out if we are using the same cloud settings - if we are then we can use a direct copy
			CloudPath cloudPathTarget = (CloudPath)target;
			if (source.getFileSystem().getCloudHostConfiguration().canOptimiseOperationsFor(cloudPathTarget)) {
				return CloudMethod.CLOUD_OPTIMISED;
			}
		}

		return CloudMethod.LOCAL_FILESYSTEM_FALLBACK;
	}

	/**
	 * Copies the remote file from one cloud to another using the local file system. This will be used when
	 * {@link CloudHostConfiguration#canCopyOrMoveInternally(CloudHostConfiguration)} returns false
	 * @see #copy(BlobStoreContext, CloudPath, Path, CopyOption...)
	 * @param context
	 * @param source
	 * @param target
	 * @param options
	 * @throws IOException
	 * TODO: This doesn't handle directory copies only single files
	 * TODO: Implement access checks
	 */
	protected void copyUsingLocalFilesystem(BlobStoreContext context, CloudPath source, Path target, Set<CopyOption> options) throws IOException {
		CloudBasicFileAttributes sourceAttributes = readAttributes(context, CloudBasicFileAttributes.class, source);

		if (sourceAttributes.isDirectory()) {
			throw new NotImplementedException("Cannot copy directories, not implemented");
		} else {
			LOG.info("Copying from local filesystem {} to target location {}...", source.toAbsolutePath(), target.toAbsolutePath());
			StandardOpenOption createOpt = options.contains(StandardCopyOption.REPLACE_EXISTING) ?
					StandardOpenOption.CREATE : StandardOpenOption.CREATE_NEW;
			LOG.debug("Opening a channel to {} with create option {}", target.toAbsolutePath(), createOpt);
	
			// Open the file to write
			try ( SeekableByteChannel targetChannel = Files.newByteChannel(target, EnumSet.of(createOpt, StandardOpenOption.WRITE)) ) {
				EnumSet<StandardOpenOption> readOptions = EnumSet.of(StandardOpenOption.READ);
				LOG.debug("Opening a channel to {} with create option {}", target.toAbsolutePath(), createOpt);
	
				// Open the file to read
				try ( CloudFileChannel sourceChannel = newByteChannel(context, source, readOptions) ) {
					// Transfer
					sourceChannel.transferTo(0L, sourceChannel.size(), targetChannel);
				}
			}
	
			LOG.info("Finished copying from local filesystem {} to target location {}", source.toAbsolutePath(), target.toAbsolutePath());
		}
	}

	/**
	 * <p>
	 * Override this method in another implementation to provide specific behaviours. This implementation uses
	 * the JClouds {@link BlobStore#copyBlob(String, String, String, String, org.jclouds.blobstore.options.CopyOptions)} interface.
	 * </p>
	 * <p>
	 * Access checks described in {@link #copy(BlobStoreContext, CloudPath, Path, Set)} are implemented here.
	 * </p>
	 * 
	 * @see #copy(BlobStoreContext, CloudPath, Path, CopyOption...)
	 * @param context
	 * @param source
	 * @param target
	 * @param options
	 * @return true if the copy succeeded, false otherwise. If false is returned then
	 * 				{@link #copyUsingLocalFilesystem(BlobStoreContext, CloudPath, Path, CopyOption...)} will be invoked from the
	 * 				{@link #copy(BlobStoreContext, CloudPath, Path, CopyOption...)} method.
	 * @throws IOException
	 */
	protected boolean copyUsingOptimisedCopy(BlobStoreContext context, CloudPath source, CloudPath target, Set<CopyOption> options) throws IOException {
		CloudBasicFileAttributes sourceAttributes = readAttributes(context, CloudBasicFileAttributes.class, source);

		if (sourceAttributes.isDirectory()) {
			checkAccess(context, source,
					options.contains(StandardCopyOption.COPY_ATTRIBUTES) ?
							COPY_DIR_SOURCE_WITH_ATTRIBUTES_PERMS : COPY_DIR_SOURCE_PERMS);
			
			if (Files.exists(target)) {
				// Check if target dir exists and replace existing hasn't been specified
				if (!options.contains(StandardCopyOption.REPLACE_EXISTING)) {
					throw new FileAlreadyExistsException("The file '" + target +
							"' already exists and the replace option was not specified");
				}
				
				// Check for non-empty target dir and recursive copy not specified
				if (!options.contains(CloudCopyOption.RECURSIVE) && newDirectoryStream(context, target, null).iterator().hasNext()) {
					throw new DirectoryNotEmptyException("The target directory '" + target.toAbsolutePath() +
							"' is not empty and the recursive copy option has not been specified");
				}

				// Check access on existing target
				checkAccess(context, target,
						options.contains(StandardCopyOption.COPY_ATTRIBUTES) ?
								COPY_DIR_TARGET_WITH_ATTRIBUTES_PERMS : COPY_DIR_TARGET_PERMS);
			} else {
				CloudPath parent = target.getParent();
				if (!Files.exists(parent)) {
					throw new FileNotFoundException("Cannot copy into a parent path that doesn't exist: " + parent);
				}

				// Check access on non-existant target, but the parent should exist...
				checkAccess(context, parent, COPY_DIR_TARGET_PARENT_PERMS);
			}

			LOG.debug("Copying directory marker from '{}' -> '{}'...",
					source.toAbsolutePath(), target.toAbsolutePath());
			try {
				context.getBlobStore().createDirectory(target.getContainerName(), target.getPathName());
			} catch (Exception e) {
				LOG.error("Internal JClouds created directory failed for '{}', will try to copy using fallback method",
						target.toAbsolutePath(), e);
				return false;
			}

			// Copy ACL's
			copyAcls(context, source, target);

			// Recursively copy the directory?
			if (options.contains(CloudCopyOption.RECURSIVE)) {
				// Copy all subpaths using the recursive directory listing
				Iterator<CloudPath> recursiveIterator = newDirectoryStream(context, source, null).iterator();
	
				LOG.debug("Copying directory '{}' and all content recursively...", source);
				while (recursiveIterator.hasNext()) {
					CloudPath nextSourcePath = recursiveIterator.next();
					CloudPath nextTargetPath = new CloudPath(target, nextSourcePath.getNameString(-1));
					copy(context, nextSourcePath, nextTargetPath, options);
				}
			}
		} else {
			checkAccess(context, source,
					options.contains(StandardCopyOption.COPY_ATTRIBUTES) ?
							COPY_FILE_SOURCE_WITH_ATTRIBUTES_PERMS : COPY_FILE_SOURCE_PERMS);

			if (Files.exists(target)) {
				checkAccess(context, target,
						options.contains(StandardCopyOption.COPY_ATTRIBUTES) ?
								COPY_FILE_TARGET_WITH_ATTRIBUTES_PERMS : COPY_FILE_TARGET_PERMS);
			} else {
				CloudPath parent = target.getParent();
				if (!Files.exists(parent)) {
					throw new FileNotFoundException("Cannot copy into a parent path that doesn't exist: " + parent);
				}
				checkAccess(context, parent, COPY_FILE_TARGET_PARENT_PERMS);
			}

			LOG.debug("Performing optimised file copy from '{}' -> '{}'...",
					source.toAbsolutePath(), target.toAbsolutePath());

			// Copy options?
			CopyOptions copyOptions = CopyOptions.NONE;
			CloudAclFileAttributes originalAttributes = null;
			if (options.contains(StandardCopyOption.COPY_ATTRIBUTES)) {
				originalAttributes = readAttributes(context, CloudAclFileAttributes.class, source);
				ContentMetadata contentMetadata =
						new ContentMetadataBuilder().contentDisposition(originalAttributes.getContentDisposition())
						 	.contentEncoding(originalAttributes.getContentEncoding())
						 	.contentLanguage(originalAttributes.getContentLanguage())
						 	.contentLength(originalAttributes.size())
						 	.contentMD5(originalAttributes.getContentMD5())
						 	.contentType(originalAttributes.getContentType())
						 	.build();
				copyOptions =
					CopyOptions.builder()
						.contentMetadata(contentMetadata)
						.userMetadata(originalAttributes.getUserMetadata())
						.build();
			}

			try {
				context.getBlobStore().copyBlob(source.getContainerName(), source.getPathName(),
						target.getContainerName(), target.getPathName(), copyOptions);
			} catch (Exception e) {
				LOG.error("Internal JClouds copy failed for '{}' -> '{}', will try to copy using fallback method",
						source.toAbsolutePath(), target.toAbsolutePath(), e);
				return false;
			}

			// Copy ACL's
			copyAcls(context, source, target);
			
			// Now apply ACL's to the copy
			if (options.contains(StandardCopyOption.COPY_ATTRIBUTES)) {
				// We can only apply the BLOB access attributes here
				Set<CloudAclEntry<PublicPrivateCloudPermissionsPrincipal>> aclEntries =
						originalAttributes.getAclSet().findAclsWithPrincipalType(PublicPrivateCloudPermissionsPrincipal.class);

				if (aclEntries != null && !aclEntries.isEmpty()) {
					LOG.debug("Setting BLOB access permissions {} for '{}'", aclEntries, target);
					aclEntries.stream().forEach(acl ->
						context.getBlobStore().setBlobAccess(target.getContainerName(), target.getPathName(),
								acl.getPrincipal().getBlobAccess()));
				}
			}

		}

		LOG.debug("Finished optimised copy from '{}' -> '{}' OK",
				source.toAbsolutePath(), target.toAbsolutePath());

		return true;
	}

	/**
	 * Copies the ACL's from the source to the target paths.
	 * @param context
	 * @param source
	 * @param target
	 * @throws IOException
	 */
	protected void copyAcls(BlobStoreContext context, CloudPath source, CloudPath target) throws IOException {
		// Set the ACL's
		CloudFileAttributesView fileAttributeView =
				getFileAttributeView(context, CloudFileAttributesView.class, source);
		if (fileAttributeView != null) {
			CloudAclFileAttributes fileAttributes = fileAttributeView.readAttributes();

			if (fileAttributes != null) {
				LOG.debug("Setting ACL's for {} to {}", target, fileAttributes);
				setAttribute(context, target, ACL_SET_ATTRIBUTE, fileAttributes.getAclSet());
			} else {
				LOG.warn("Cannot set ACL's for {}, no ACL file attributes are available", target);
			}

		} else {
			LOG.warn("Cannot set ACL's for {}, no file attributes view available", target);
		}
	}

	@Override
	public CloudMethod move(BlobStoreContext context, CloudPath source, Path target, Set<CopyOption> options) throws IOException {
		LOG.info("Performing move operation from '{}' -> '{}' using copy/delete", source, target);
		CloudMethod method = copy(context, source, target, options);
		delete(context, source, options.contains(CloudCopyOption.RECURSIVE) ?
				EnumSet.of(DeleteOption.RECURSIVE) : EnumSet.noneOf(DeleteOption.class));
		return method;
	}

	/**
	 * Retrieves the {@link #getFileAttributeView(BlobStoreContext, Class, CloudPath) CloudFileAttributesView} and
	 * then invokes {@link CloudFileAttributesView#checkAccess(Set)}
	 */
	@Override
	public void checkAccess(BlobStoreContext context, CloudPath path, Set<AclEntryPermission> checkPermissions) throws IOException {
		CloudFileAttributesView fileAttributeView = getFileAttributeView(context, CloudFileAttributesView.class, path);
		fileAttributeView.checkAccess(checkPermissions);
	}

	/**
	 * Can return a {@link CloudFileAttributesView}
	 * @param type {@link CloudFileAttributesView} or {@link BasicFileAttributeView}
	 */
	@Override
	public <V extends FileAttributeView> V getFileAttributeView(BlobStoreContext blobStoreContext, Class<V> type, CloudPath cloudPath) {
		if (CloudFileAttributesView.class.equals(type) || BasicFileAttributeView.class.equals(type)) {
			return type.cast(new CloudFileAttributesView(blobStoreContext, cloudPath));
		}

		return null;
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(BlobStoreContext blobStoreContext, Class<A> type,
			CloudPath cloudPath) throws IOException {
		CloudFileAttributesView fileAttributeView = getFileAttributeView(blobStoreContext, CloudFileAttributesView.class, cloudPath);

		if (fileAttributeView != null) {
			if (type.equals(BasicFileAttributes.class) || type.equals(CloudBasicFileAttributes.class)) {
				return type.cast(fileAttributeView.readAttributes());
			} else if (type.equals(CloudAclFileAttributes.class)) {
				return type.cast(fileAttributeView.readAttributes());
			}
		}

		return null;
	}

	@Override
	public Map<String, Object> readAttributes(BlobStoreContext context, CloudPath path, String attributes) throws IOException {
		if (StringUtils.isBlank(attributes)) {
			throw new IllegalArgumentException("Attributes to read is empty!");
		}
		
		String fileView = CloudFileAttributesView.VIEW_NAME;
		if (StringUtils.contains(attributes, ":")) {
			fileView = StringUtils.substringBefore(attributes, ":");
			attributes = StringUtils.substringAfter(attributes, ":");
		}

		if (!fileView.equals(CloudFileAttributesView.VIEW_NAME) && !fileView.equals("basic")) {
			throw new IllegalArgumentException("Unsupported file attributes view '" + fileView +
					"' from attributes string: " + attributes);
		}

		String[] attributesToGet = StringUtils.split(attributes, ",");
		boolean getAclAttributeView = StringUtils.equals(attributes, "*") ||
				(attributesToGet != null &&
					Arrays.stream(attributesToGet)
						.anyMatch(a -> StringUtils.containsAny(a, ACL_SET_ATTRIBUTE)));

		CloudBasicFileAttributes readAttributes =
				getAclAttributeView ?
						readAttributes(context, CloudAclFileAttributes.class, path) :
						readAttributes(context, CloudBasicFileAttributes.class, path);
		Map<String,Object> mapAttributes = new HashMap<>();
		mapAttributes.put("lastAccessTime", readAttributes.lastAccessTime());
		mapAttributes.put("lastModifiedTime", readAttributes.lastModifiedTime());
		mapAttributes.put("fileKey", readAttributes.fileKey());
		mapAttributes.put("creationTime", readAttributes.creationTime());
		mapAttributes.put("contentDisposition", readAttributes.getContentDisposition());
		mapAttributes.put("contentEncoding", readAttributes.getContentEncoding());
		mapAttributes.put("contentExpires", readAttributes.getContentExpires());
		mapAttributes.put("contentLanguage", readAttributes.getContentLanguage());
		mapAttributes.put("contentMD5", readAttributes.getContentMD5());
		mapAttributes.put("contentType", readAttributes.getContentType());
		mapAttributes.put("eTag", readAttributes.getETag());
		mapAttributes.put("physicalLocation", readAttributes.getPhysicalLocation());
		mapAttributes.put("uri", readAttributes.getUri());
		mapAttributes.put("userMetadata", readAttributes.getUserMetadata());
		
		if (getAclAttributeView) {
			CloudAclFileAttributes aclReadAttributes = (CloudAclFileAttributes)readAttributes;
			mapAttributes.put(ACL_SET_ATTRIBUTE, aclReadAttributes.getAclSet());
		}
		
		return mapAttributes;
	}

	/**
	 * Allows for the <em>aclSet</em> of type {@link CloudAclEntrySet} to be set as an attribute
	 * @throws IOException 
	 */
	@Override
	public void setAttribute(BlobStoreContext context, CloudPath path, String attribute, Object value) throws IOException {
		if (attribute == ACL_SET_ATTRIBUTE && value instanceof CloudAclEntrySet) {
			CloudFileAttributesView fileAttributeView = getFileAttributeView(context, CloudFileAttributesView.class, path);
			fileAttributeView.setAclFileAttributes((CloudAclEntrySet)value);
			return;
		}

		throw new UnsupportedOperationException("Unknown/unsettable file attribute '" + attribute +
				"', can only set " + ACL_SET_ATTRIBUTE + " of type " + CloudAclEntrySet.class);
	}

}
