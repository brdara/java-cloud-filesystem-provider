package com.uk.xarixa.cloud.filesystem.core.io;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.domain.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.AclConstants;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntrySet;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.NotOwnerException;
import com.uk.xarixa.cloud.filesystem.core.nio.options.CloudCopyOption;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

/**
 * Implements a {@link File} as a cloud file based on a {@link CloudPath}.
 */
public class CloudFile extends File implements AclConstants {
	private static final long serialVersionUID = -4308942797259163855L;
	private static final EnumSet<StorageType> DIRECTORY_STORAGE_TYPES = EnumSet.of(StorageType.FOLDER, StorageType.CONTAINER);
	private static final File[] EMPTY_FILE_ARRAY = new File[0];
	private final static Logger LOG = LoggerFactory.getLogger(CloudFile.class);
	private final CloudPath cloudPath;
	private final static List<CloudPath> deleteOnExit = new ArrayList<>();

	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
		    	deleteOnExit.stream().forEach(cloudPath -> new CloudFile(cloudPath).delete());
		    }
		});
	}

	public CloudFile(CloudPath cloudPath) {
		super(CloudPath.DEFAULT_PATH_SEPARATOR);
		this.cloudPath = cloudPath;
	}

	@Override
	public String getName() {
		return cloudPath.getFileName().toString();
	}

	@Override
	public String getParent() {
		return cloudPath.getParent().toString();
	}

	@Override
	public File getParentFile() {
		return new CloudFile(cloudPath.getParent());
	}

	@Override
	public String getPath() {
		return cloudPath.toString();
	}

	@Override
	public boolean isAbsolute() {
		return cloudPath.isAbsolute();
	}

	@Override
	public String getAbsolutePath() {
		return cloudPath.toAbsolutePath().toString();
	}

	@Override
	public File getAbsoluteFile() {
		return new CloudFile(cloudPath.getParent().toAbsolutePath());
	}

	@Override
	public String getCanonicalPath() throws IOException {
		return cloudPath.normalize().toString();
	}

	@Override
	public File getCanonicalFile() throws IOException {
		return new CloudFile(cloudPath.normalize());
	}

	@Override
	public URL toURL() throws MalformedURLException {
		return cloudPath.toUri().toURL();
	}

	@Override
	public URI toURI() {
		return cloudPath.toUri();
	}

	@Override
	public boolean exists() {
		try {
			return cloudPath.exists();
		} catch (IOException e) {
			LOG.warn("An error occurred checking if the cloud path '{}' exists", cloudPath.toAbsolutePath(), e);
			return false;
		}
	}

	@Override
	public boolean isDirectory() {
		return isStorageType(DIRECTORY_STORAGE_TYPES);
	}

	boolean isStorageType(EnumSet<StorageType> storageTypes) {
		return isStorageType(readFileAttributes(), storageTypes);
	}
		
	boolean isStorageType(CloudBasicFileAttributes readAttributes, EnumSet<StorageType> storageTypes) {
		// This path doesn't exist in the FS
		if (readAttributes == null) {
			return false;
		}

		StorageType storageType = readAttributes.getStorageType();
		return readAttributes == null || storageType == null? false :
			storageTypes.contains(storageType);
	}

	CloudBasicFileAttributes readFileAttributes() {
		CloudBasicFileAttributes readAttributes;
		try {
			readAttributes = Files.readAttributes(cloudPath, CloudBasicFileAttributes.class);
		} catch (IOException e) {
			LOG.debug("Unable to read file attributes for '{}'", cloudPath, e);
			return null;
		}

		return readAttributes;
	}

	@Override
	public boolean isFile() {
		return isStorageType(EnumSet.of(StorageType.BLOB));
	}

	/**
	 * Hidden files are not enabled in the cloud
	 */
	@Override
	public boolean isHidden() {
		return false;
	}

	/**
	 * Returns 0 for a container with no path
	 * @return
	 */
	@Override
	public long lastModified() {
		// Is this just a container?
		if (StringUtils.isEmpty(cloudPath.getPathName())) {
			return 0L;
		}

		CloudBasicFileAttributes readAttributes = readFileAttributes();
		return readAttributes == null ? 0L : readAttributes.lastModifiedTime().toMillis();
	}

	@Override
	public long length() {
		// Is this just a container?
		if (StringUtils.isEmpty(cloudPath.getPathName())) {
			return 0L;
		}

		CloudBasicFileAttributes readAttributes = readFileAttributes();
		return readAttributes == null ? 0L : readAttributes.size();
	}

	@Override
	public boolean createNewFile() throws IOException {
		try {
			Files.createFile(cloudPath);
			return true;
		} catch (FileAlreadyExistsException e) {
			return false;
		}
	}

	@Override
	public boolean delete() {
		try {
			Files.delete(cloudPath);
			return true;
		} catch (IOException e) {
			LOG.debug("Could not delete '{}'", cloudPath, e);
			return false;
		}
	}

	@Override
	public void deleteOnExit() {
		deleteOnExit.add(cloudPath);
	}

	@Override
	public String[] list() {
		return list(TrueFileFilter.TRUE);
	}

	@Override
	public String[] list(FilenameFilter filter) {
		List<String> listing = listing(filter);
		return listing == null ? null : listing.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
	}
	
	public List<String> listing(FilenameFilter filter) {
		try {
			return listing(filter, null, String.class);
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * @param filenameFilter
	 * @param returnType	Either a {@link String}, {@link File} or {@link CloudFile} type
	 * @return
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public <T extends Object> List<T> listing(FilenameFilter filenameFilter, FileFilter fileFilter, Class<T> returnType)
			throws IOException {
		if (!returnType.equals(String.class) && !returnType.equals(File.class) && !returnType.equals(CloudFile.class)) {
			throw new IllegalArgumentException("Cannot return the type " + returnType);
		}

		if (!isDirectory() || !exists()) {
			return null;
		}

		@SuppressWarnings("rawtypes")
		Filter cloudPathFilter = new DirectoryStream.Filter<CloudPath>() {
			@Override
			public boolean accept(CloudPath entry) throws IOException {
				if (filenameFilter != null) {
					return filenameFilter.accept(new CloudFile(entry.getParent()), entry.getNameString(-1));
				} else if (fileFilter != null) {
					return fileFilter.accept(new CloudFile(entry));
				}

				// Default to true
				return true;
			}
		};

		DirectoryStream<Path> dirStream = Files.newDirectoryStream(cloudPath, cloudPathFilter);
		List<T> filenames = new ArrayList<>();
		
		// Iteratively perform a file listing
		Iterator<Path> iterator = dirStream.iterator();
		while (iterator.hasNext()) {
			CloudPath nextPath = (CloudPath)iterator.next();

			// Create object of the appropriate type
			T filenameObj;
			if (returnType.equals(String.class)) {
				filenameObj = (T)nextPath.toAbsolutePath().toString();
			} else {
				filenameObj = (T)new CloudFile(nextPath);
			}
			
			filenames.add(filenameObj);
		}
		
		return filenames;
	}

	@Override
	public File[] listFiles() {
		return listFiles((FileFilter)TrueFileFilter.TRUE);
	}

	@Override
	public File[] listFiles(FilenameFilter filter) {
		List<File> listing;
		try {
			listing = listing(filter, null, File.class);
		} catch (IOException e) {
			return null;
		}

		return listing == null ? null : listing.toArray(EMPTY_FILE_ARRAY);
	}

	@Override
	public File[] listFiles(FileFilter filter) {
		List<File> listing;
		try {
			listing = listing(null, filter, File.class);
		} catch (IOException e) {
			return null;
		}

		return listing == null ? null : listing.toArray(EMPTY_FILE_ARRAY);
	}

	@Override
	public boolean mkdir() {
		return mkdir(false);
	}
	
	/**
	 * Performs directory creation
	 * @param recursive	true is equivalent to {@link #mkdirs()}, false to {@link #mkdir()}
	 * @return
	 */
	public boolean mkdir(boolean recursive) {
		if (recursive) {
			try {
				Files.createDirectories(cloudPath);
			} catch (IOException e) {
				LOG.warn("Could not create directories recursively at path '{}'", cloudPath.toAbsolutePath(), e);
				return false;
			}
		} else {
			try {
				Files.createDirectory(cloudPath);
			} catch (IOException e) {
				LOG.warn("Could not create directory at path '{}'", cloudPath.toAbsolutePath(), e);
				return false;
			}
		}
		
		return true;
	}

	@Override
	public boolean mkdirs() {
		return mkdir(true);
	}

	@Override
	public boolean renameTo(File dest) {
		Path target = dest.toPath();

		try {
			Files.move(cloudPath, target, CloudCopyOption.DONT_RETURN_COPY_METHOD);
		} catch (IOException e) {
			LOG.warn("Could not rename '{}' to '{}'", cloudPath.toAbsolutePath(), target.toAbsolutePath(), e);
			return false;
		}

		return true;
	}

	@Override
	public boolean setLastModified(long time) {
		try {
			Files.setLastModifiedTime(cloudPath, FileTime.from(time, TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			LOG.warn("Could not set last modified time for '{}'", cloudPath.toAbsolutePath(), e);
			return false;
		}

		return true;
	}

	@Override
	public boolean setReadOnly() {
		return setWritable(false) && setReadable(true);
	}

	@Override
	public boolean setWritable(boolean writable, boolean ownerOnly) {
		try {
			return setPermissionsForCurrentCloudPath(ownerOnly, writable ? AclEntryType.ALLOW : AclEntryType.DENY,
					ALL_FILE_WRITE_PERMISSIONS, ALL_DIRECTORY_WRITE_PERMISSIONS);
		} catch (NotOwnerException e) {
			LOG.warn("Cannot set write state, current user does not own the file ACL for {}", cloudPath);
			return false;
		}
	}

	boolean setPermissionsForCurrentCloudPath(boolean ownerOnly, AclEntryType type,
			EnumSet<AclEntryPermission> filePermissions, EnumSet<AclEntryPermission> directoryPermissions) throws NotOwnerException {
		// Get the existing ACL's
		CloudFileAttributesView cloudFileAttributeView = Files.getFileAttributeView(cloudPath, CloudFileAttributesView.class);
		CloudAclFileAttributes aclFileAttributes;
		try {
			aclFileAttributes = cloudFileAttributeView.readAttributes();
		} catch (IOException e) {
			LOG.warn("Could not read ACL file attributes for {}", cloudPath.toString());
			return false;
		}

		// Get the current set of ACL's
		CloudAclEntrySet aclSet = aclFileAttributes.getAclSet();

		// Build a new ACL
		CloudAclEntryBuilder<UserPrincipal> aclBuilder = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class);
		aclBuilder.setType(type);

		// Add permissions based on the file type (directory/file)
		boolean isDirectoryPath = isStorageType(aclFileAttributes, DIRECTORY_STORAGE_TYPES);
		EnumSet<AclEntryPermission> permissions;
		if (isDirectoryPath) {
			permissions = directoryPermissions;
		} else {
			permissions = filePermissions;
		}
		aclBuilder.addPermissions(permissions);

		// Get this user who should be allowed to modify the ACL
		UserPrincipal aclOwnerPrincipal = AnonymousUserPrincipal.INSTANCE;
		UserGroupLookupService<?> userGroupLookupService =
				cloudPath.getFileSystem().getCloudHostConfiguration().getUserGroupLookupService();
		if (userGroupLookupService == null) {
			LOG.warn("No {} is avalable from cloud host configuration {}, will attempt to change ACL's with anonymous user",
					UserGroupLookupService.class, cloudPath.getFileSystem().getCloudHostConfiguration());
		} else {
			aclOwnerPrincipal = userGroupLookupService.getCurrentUser();
		}

		// Get the current user who we will pass in as owners of the ACL
		if (aclOwnerPrincipal == null) {
			LOG.warn("No current user is avalable from user group lookup service {}, will attempt to change ACL's with anonymous user",
					userGroupLookupService);
			aclOwnerPrincipal = AnonymousUserPrincipal.INSTANCE;
		}

		UserPrincipal[] userPrincipals;
		if (ownerOnly) {
			// The owners of the ACL should be given the rights as they are the file owners
			userPrincipals = aclFileAttributes.getOwners().toArray(USER_PRINCIPAL_EMPTY_ARRAY);
		} else {
			// This applies to each and every user
			userPrincipals = new UserPrincipal[] {AnonymousUserPrincipal.INSTANCE};
		}

		// Add all entries
		addAclEntryForEach(aclOwnerPrincipal, aclSet, aclBuilder, userPrincipals);

		// Set the ACL's
		Set<CloudAclEntry<?>> unsetAclAttributes;
		try {
			unsetAclAttributes = cloudFileAttributeView.setAclFileAttributes(aclSet);
		} catch (Exception e) {
			LOG.warn("Could not set status for type={}, owner={} on path '{}' with file permissions {}",
					type, ownerOnly, cloudPath.toAbsolutePath(), isDirectoryPath ? directoryPermissions : filePermissions, e);
			return false;
		}
		
		// Are any of the unset ACL's applicable
		// Same type for a user?
		// Check if this is an ACL permission which we tried to set
		if (unsetAclAttributes.stream().anyMatch(a ->
				type.equals(a.getType()) && Arrays.stream(userPrincipals).anyMatch(p -> p.equals(a.getPrincipal())) &&
						a.getPermissions().stream().anyMatch(p -> permissions.contains(p)))) {
			LOG.warn("Not all ACL's were set for the file path {}, these remained unset {}, operation failed",
					cloudPath, unsetAclAttributes);
			return false;
		}
		
		return true;
	}

	protected void addAclEntryForEach(Principal aclOwner, CloudAclEntrySet aclSet,
			CloudAclEntryBuilder<UserPrincipal> builderTemplate, UserPrincipal... principals) throws NotOwnerException {
		// For each owner do this
		for (UserPrincipal principal : principals) {
			// Add the principal
			CloudAclEntryBuilder<UserPrincipal> principalBuilder = builderTemplate.clone();
			CloudAclEntry<UserPrincipal> aclEntry = principalBuilder.setPrincipal(principal).build();

			// Add the ACL using the force parameter, which removes any conflicts
			aclSet.addAclEntry(aclOwner, aclEntry, true);
		}
	}

	@Override
	public boolean setWritable(boolean writable) {
		return setWritable(writable, false);
	}

	@Override
	public boolean setReadable(boolean readable, boolean ownerOnly) {
		try {
			return setPermissionsForCurrentCloudPath(ownerOnly, readable ? AclEntryType.ALLOW : AclEntryType.DENY,
					ALL_FILE_READ_PERMISSIONS, ALL_DIRECTORY_READ_PERMISSIONS);
		} catch (NotOwnerException e) {
			LOG.warn("Cannot set read state, current user does not own the file ACL for {}", cloudPath);
			return false;
		}
	}

	@Override
	public boolean setReadable(boolean readable) {
		return setReadable(readable, false);
	}

	@Override
	public boolean setExecutable(boolean executable, boolean ownerOnly) {
		try {
			return setPermissionsForCurrentCloudPath(ownerOnly, executable ? AclEntryType.ALLOW : AclEntryType.DENY,
					ALL_FILE_EXEC_PERMISSIONS, ALL_FILE_EXEC_PERMISSIONS);
		} catch (NotOwnerException e) {
			LOG.warn("Cannot set execute state, current user does not own the file ACL for {}", cloudPath);
			return false;
		}
	}

	@Override
	public boolean setExecutable(boolean executable) {
		return setExecutable(executable, false);
	}

	@Override
	public boolean canRead() {
		return hasPermissions(EnumSet.of(AclEntryPermission.READ_DATA));
	}

	@Override
	public boolean canWrite() {
		return hasPermissions(EnumSet.of(AclEntryPermission.WRITE_DATA));
	}

	@Override
	public boolean canExecute() {
		return hasPermissions(EnumSet.of(AclEntryPermission.EXECUTE));
	}

	boolean hasPermissions(EnumSet<AclEntryPermission> permissions) {
		CloudHostConfiguration cloudHostConfiguration = cloudPath.getFileSystem().getCloudHostConfiguration();
		CloudHostSecurityManager cloudHostSecurityManager = cloudHostConfiguration.getCloudHostSecurityManager();
		UserGroupLookupService<?> userPrincipalLookupService =
				cloudHostConfiguration.getUserGroupLookupService();

		if (cloudHostSecurityManager == null) {
			// No security manager, allow access
			LOG.debug("No {} found in cloud host configuration {}, default action is to allow all access",
					CloudHostSecurityManager.class, cloudHostConfiguration);
			return true;
		}

		UserPrincipal currentUser = null; // Anonymous
		if (userPrincipalLookupService != null) {
			currentUser = userPrincipalLookupService.getCurrentUser();
		} else {
			LOG.debug("Cannot find current user, no {} in cloud host configuration {}",
					UserGroupLookupService.class, cloudHostConfiguration);
			currentUser = AnonymousUserPrincipal.INSTANCE;
		}

		return cloudHostSecurityManager.checkAccessAllowed(cloudPath, currentUser, permissions);
	}

	@Override
	public long getTotalSpace() {
		return 0L;
	}

	@Override
	public long getFreeSpace() {
		return 0L;
	}

	@Override
	public long getUsableSpace() {
		return 0L;
	}

	@Override
	public int compareTo(File pathname) {
		if (!(pathname instanceof CloudFile)) {
			return 1;
		}

		return ((CloudFile)pathname).cloudPath.compareTo(cloudPath);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof CloudFile)) {
			return false;
		}

		return ((CloudFile)obj).cloudPath.equals(cloudPath);
	}

	@Override
	public int hashCode() {
		return cloudPath.hashCode();
	}

	@Override
	public String toString() {
		return cloudPath.toAbsolutePath().toString();
	}

	@Override
	public Path toPath() {
		return cloudPath;
	}

}
