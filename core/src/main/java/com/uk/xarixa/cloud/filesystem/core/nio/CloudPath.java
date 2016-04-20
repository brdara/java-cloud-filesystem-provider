package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.io.CloudFile;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;
import com.uk.xarixa.cloud.filesystem.core.utils.PathIterator;

/**
 * Implementation of {@link Path} for cloud based filesystems.
 */
public class CloudPath implements Path {
	public static final char PATH_SEPARATOR_CHAR = '/';
	public static final String PATH_SEPARATOR = "/";
	private static final Modifier[] EMPTY_MODIFIERS_ARRAY = new Modifier[0];
	private static final Logger LOG = LoggerFactory.getLogger(CloudPath.class);
	private final CloudFileSystem fileSystem;
	private final boolean isRoot;
	private final boolean isAbsolute;
	private final List<String> deconstructedPath;
	private final List<String> deconstructedRootPath;
	private String originalFullPath;

	public CloudPath(CloudFileSystem fileSystem, boolean isAbsolute, List<String> rootPath, List<String> fullPath) {
		this.deconstructedPath = fullPath;
		this.deconstructedRootPath = rootPath;
		this.fileSystem = fileSystem;
		this.isAbsolute = isAbsolute;
		this.isRoot = isRootPath();
		this.originalFullPath = PATH_SEPARATOR + StringUtils.join(fullPath, PATH_SEPARATOR);

		// No root path means we just add a slash to this
//		if (!isAbsolute && (rootPath == null || rootPath.isEmpty())) {
//			throw new IllegalArgumentException("No reference to a root path with a relative path argument " + originalFullPath);
//		}
		
		if (deconstructedPath == null || deconstructedPath.isEmpty()) {
			throw new IllegalArgumentException("Cannot get a root path from path elements " + originalFullPath +
					", it must start with " + PATH_SEPARATOR + " and have at least 1 path element");
		}
	}

	public CloudPath(CloudFileSystem fileSystem, boolean isAbsolute, List<String> fullPath) {
		this(fileSystem, isAbsolute, null, fullPath);
	}

	public CloudPath(CloudFileSystem fileSystem, boolean isAbsolute, String fullPath) {
		this(fileSystem, isAbsolute, deconstructPath(fullPath));
		this.originalFullPath = StringUtils.removeEnd(fullPath, PATH_SEPARATOR);
	}
	
	public CloudPath(CloudFileSystem fileSystem, boolean isAbsolute, CloudPath rootPath, String fullPath) {
		this(fileSystem, isAbsolute, rootPath.getAllPaths(), deconstructPath(fullPath));
		this.originalFullPath = StringUtils.removeEnd(fullPath, PATH_SEPARATOR);
	}

	public CloudPath(CloudFileSystem fileSystem, boolean isAbsolute, String rootPath, String fullPath) {
		this(fileSystem, isAbsolute, deconstructPath(rootPath), deconstructPath(fullPath));
		this.originalFullPath = StringUtils.removeEnd(fullPath, PATH_SEPARATOR);
	}

	public CloudPath(CloudPath cloudPath, String fullPath) {
		this(cloudPath.getFileSystem(), false, cloudPath.getAllPaths(), deconstructPath(fullPath));
	}

	/**
	 * Creates a non-absolute path
	 * @param cloudPath
	 * @param rootPath
	 * @param partialPath
	 */
	public CloudPath(CloudPath cloudPath, boolean isAbsolute, List<String> rootPath, List<String> partialPath) {
		this(cloudPath.fileSystem, isAbsolute, rootPath, partialPath);
	}

	protected static List<String> deconstructPath(String fullPath) {
		if (fullPath == null) {
			return null;
		}

		List<String> rootPaths = new ArrayList<String>();
		PathIterator pathIter = new PathIterator(StringUtils.removeEnd(fullPath, PATH_SEPARATOR));

		while (pathIter.hasNext()) {
			String pathPart = pathIter.next();

			if (StringUtils.isEmpty(pathPart)) {
				LOG.warn("Path part {} of the root paths of '{}' is an empty folder name '{}', ignoring",
						rootPaths.size(), fullPath, pathPart);
			} else if (StringUtils.isBlank(pathPart)) {
				// TODO: Expand this check for valid folder names based on the provider scheme (eg for S3, Azure, etc.)
				throw new IllegalArgumentException("Path part " + rootPaths.size() +
						" of the root paths of '" + fullPath + "' is an empty folder name '" + pathPart + "'");
			} else if (pathPart.equals("..")) {
				// A ".." denotes go up a level
				if (rootPaths.size() > 0) {
					rootPaths.remove(rootPaths.size() - 1);
				}
			} else if (!pathPart.equals(".")) {
				// A "." denotes the same directory, so ignore it
				rootPaths.add(pathPart);
			}
		}

		return rootPaths;
	}

	boolean isRootPath() {
		return isAbsolute && deconstructedPath.size() <= 1;
	}

	@Override
	public CloudFileSystem getFileSystem() {
		return fileSystem;
	}

	@Override
	public boolean isAbsolute() {
		return isAbsolute;
	}

	@Override
	public Path getRoot() {
		if (isRoot) {
			return null;
		}

		if (isAbsolute) {
			// For an absolute path take the deconstructedPath
			if (deconstructedPath.size() > 1) {
				return new CloudPath(this, true, null, constructPath(deconstructedPath, 0, 1));
			}			
		} else {
			// For a relative path take the deconstructedRootPath if the deconstructedPath has elements
			if (deconstructedPath.size() + deconstructedRootPath.size() > 1 && deconstructedRootPath.size() > 0) {
				return new CloudPath(this, true, null, constructPath(deconstructedRootPath, 0, 1));
			}
		}
		
		// This is a root path
		return null;
	}
	
	/**
	 * Constructs a path string
	 * @param startElement	The first element in {@link #deconstructPath(String)}
	 * @param numberOfElements The number of elements
	 * @return
	 */
	protected List<String> constructPath(List<String> pathsList, int startElement, int numberOfElements) {
		if (numberOfElements < 1) {
			throw new IllegalArgumentException("Illegal number of elements to construct a path from " + numberOfElements);
		}
		
		int endElement = startElement + numberOfElements;
		if (endElement > pathsList.size()) {
			throw new IllegalArgumentException("Cannot construct a path with " + numberOfElements +
					", there are only " + pathsList.size() + " elements in the path: " + pathsList.toString());
		}

		return pathsList.subList(startElement, endElement);
	}

	@Override
	public Path getFileName() {
		int sizeMinusOne = deconstructedPath.size() - 1;
		List<String> rootPath = deconstructedPath.subList(0, sizeMinusOne);
		List<String> filename = new ArrayList<String>(1);
		filename.add(deconstructedPath.get(sizeMinusOne));
		return new CloudPath(this, false, rootPath, filename);
	}

	@Override
	public CloudPath getParent() {
		List<String> pathsList = getAllPaths();
		
		if (pathsList.size() <= 1) {
			return null;
		}

		return new CloudPath(this, true, null, constructPath(pathsList, 0, pathsList.size() - 1));
	}

	@Override
	public int getNameCount() {
		return getTotalPathCount() - 1;
	}
	
	protected int getTotalPathCount() {
		return deconstructedRootPath != null ? deconstructedRootPath.size() + deconstructedPath.size() : deconstructedPath.size();
	}

	@Override
	public CloudPath getName(int index) {
		int nameCount = getTotalPathCount();
		int realIndex = indexArgToRealIndex(index);
		checkIndex(nameCount, realIndex);
		List<String> allPaths = getAllPaths();
		List<String> rootPaths = realIndex > 0 ? allPaths.subList(0, realIndex) : null;
		return new CloudPath(this, rootPaths == null, rootPaths, constructPath(allPaths, realIndex, 1));
	}
	
	/**
	 * Similar to {@link #getName(int)} but returns the string equivalent of the path component at the index
	 * @param index
	 * @return
	 */
	public String getNameString(int index) {
		int nameCount = getTotalPathCount();
		int realIndex = indexArgToRealIndex(index);
		checkIndex(nameCount, realIndex);
		List<String> allPaths = getAllPaths();
		return allPaths.get(realIndex);
	}

	protected int indexArgToRealIndex(int index) {
		if (index >= 0) {
			return index;
		}

		List<String> allPaths = getAllPaths();
		return allPaths.size() + index;
	}
	
	/**
	 * Creates an iterator across all of the paths {@link #deconstructedRootPath} + {@link #deconstructedPath}
	 * @return
	 */
	public List<String> getAllPaths() {
		if (isAbsolute) {
			return deconstructedPath;
		}

		List<String> allPaths = new ArrayList<String>();
		allPaths.addAll(deconstructedRootPath);
		allPaths.addAll(deconstructedPath);
		return allPaths;
	}

	protected void checkIndex(int nameCount, int index) {
		if (nameCount == 0 || index > nameCount || index < 0) {
			throw new IllegalArgumentException("Cannot get name index '" + index +
					"' from path '" + getAllPaths() + "' (" + nameCount + " total path indexes)");
		}
	}

	@Override
	public CloudPath subpath(int beginIndex, int endIndex) {
		int nameCount = getTotalPathCount();
		beginIndex = indexArgToRealIndex(beginIndex);
		endIndex = indexArgToRealIndex(endIndex);
		checkIndex(nameCount, beginIndex);
		checkIndex(nameCount, endIndex);
		
		if (beginIndex >= nameCount) {
			throw new IllegalArgumentException("The subpath start index " + beginIndex + " must be less than the total paths " +
					nameCount + " for path '" + toString() + "'");
		}
		
		if (endIndex < beginIndex) {
			throw new IllegalArgumentException("The subpath end index " + endIndex + " must be greater than the start index " +
					beginIndex + " for path '" + toString() + "'");
		}
		
		List<String> allPaths = getAllPaths();
		List<String> rootPaths = beginIndex > 0 ? allPaths.subList(0, beginIndex) : null;
		List<String> relativePath = allPaths.subList(beginIndex, endIndex + 1);
		return new CloudPath(this, rootPaths == null, rootPaths, relativePath);
	}

	@Override
	public boolean startsWith(Path other) {
		boolean canComparePaths = testCanComparePaths(other);
		return canComparePaths ? startsWithInternal(((CloudPath)other).getAllPaths()) : false;
	}

	protected boolean testCanComparePaths(Path other) {
		if (getFileSystem() != other.getFileSystem()) {
			return false;
		}

		if (!(other instanceof CloudPath)) {
			return false;
		}

		if (getNameCount() < other.getNameCount()) {
			return false;
		}
		
		return true;
	}


	private boolean startsWithInternal(List<String> otherAllPaths) {
		List<String> allPaths = getAllPaths();

		// Iterate over the paths
		for (int i=0; i<otherAllPaths.size(); i++) {
			if (!StringUtils.equals(allPaths.get(i), otherAllPaths.get(i))) {
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean startsWith(String other) {
		List<String> deconstructedPath = deconstructPath(StringUtils.removeEnd(other, PATH_SEPARATOR));
		return startsWithInternal(deconstructedPath);
	}

	@Override
	public boolean endsWith(Path other) {
		boolean canComparePaths = testCanComparePaths(other);
		return canComparePaths ? endsWithInternal(((CloudPath)other).getAllPaths()) : false;
	}

	private boolean endsWithInternal(List<String> otherAllPaths) {
		List<String> allPaths = getAllPaths();
		int myPathsCounter = -1;

		for (int i=otherAllPaths.size() - 1; i>=0; i--) {
			if (!StringUtils.equals(allPaths.get(indexArgToRealIndex(myPathsCounter)), otherAllPaths.get(i))) {
				return false;
			}

			myPathsCounter--;
		}

		return true;
	}

	@Override
	public boolean endsWith(String other) {
		List<String> deconstructedPath = deconstructPath(StringUtils.removeEnd(other, PATH_SEPARATOR));
		return endsWithInternal(deconstructedPath);
	}

	/**
	 * Has no effect, the cloud paths are always de-normalised so this method returns this instance
	 */
	@Override
	public CloudPath normalize() {
		return this;
	}

	@Override
	public Path resolve(Path other) {
		if (other.isAbsolute()) {
			return other;
		}
		
		if (!(other instanceof CloudPath)) {
			LOG.warn("Attempt to resolve a non CloudPath instance of type {} with path {} against this path {}, " +
					"returning the other path", other.getClass().getName(), other.toString(), toString());
			return other;
		}
		
		// Return the joined paths
		return resolveInternal((CloudPath)other);
	}
	
	private Path resolveInternal(CloudPath other) {
		if (other.isAbsolute) {
			return other;
		}
		
		// Other is a relative path so join it to this one
		List<String> allPaths = getAllPaths();
		for (String otherPath : other.deconstructedPath) {
			allPaths.add(otherPath);
		}

		return new CloudPath(this, true, null, allPaths);
	}

	@Override
	public Path resolve(String other) {
		return resolveInternal(new CloudPath(this.fileSystem, other.startsWith(PATH_SEPARATOR), other));
	}

	@Override
	public Path resolveSibling(Path other) {
		return getParent().resolve(other);
	}

	@Override
	public Path resolveSibling(String other) {
		return getParent().resolve(other);
	}

	@Override
	public Path relativize(Path other) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public URI toUri() {
		String userPath = toAbsolutePath().toString();
		String hostname = fileSystem.getCloudHostConfiguration().getName();

		try {
			return new URI(CloudFileSystemProviderDelegate.CLOUD_SCHEME, hostname, userPath);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Cannot create a URI for this path '" + userPath + "' and host '" + hostname + "'", e);
		}
	}

	@Override
	public CloudPath toAbsolutePath() {
		if (isAbsolute) {
			return this;
		}

		return new CloudPath(fileSystem, true, null, getAllPaths());
	}
	
	/**
	 * Symlinks are not allowed
	 */
	@Override
	public CloudPath toRealPath(LinkOption... options) throws IOException {
		if (Arrays.stream(options).anyMatch(p -> p.equals(LinkOption.NOFOLLOW_LINKS))) {
			LOG.warn("Ignoring " + LinkOption.NOFOLLOW_LINKS + ", symlinks are not allowed in Cloud based filesystems");
		}
		
		if (!exists()) {
			throw new FileNotFoundException("The filesystem path '" + toAbsolutePath().toString() + "' does not exist");
		}

		return this;
	}
	
	/**
	 * @see Files#exists(Path, LinkOption...)
	 * @return
	 * @throws IOException 
	 */
	public boolean exists() throws IOException {
		return Files.exists(this);
	}

	/**
	 * Returns the cloud container name for this path
	 * @return
	 */
	public String getContainerName() {
		return getAllPaths().get(0);
	}
	
	/**
	 * Returns the cloud path name, i.e. without the container name
	 * @return	The path, if there is one, or null if this is just a container
	 * @see	#getContainerName()
	 */
	public String getPathName() {
		CloudPath absolutePath = toAbsolutePath();
		if (absolutePath.deconstructedPath.size() <= 1) {
			return null;
		}

		return absolutePath.subpath(1, -1).toString();
	}
	

	@Override
	public File toFile() {
		return new CloudFile(this);
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException {
		if (!CloudWatchService.class.isAssignableFrom(watcher.getClass())) {
			throw new IllegalArgumentException("The file system watcher must be of type " +
					CloudWatchService.class + " but was " + watcher.getClass());
		}

		return ((CloudWatchService)watcher).register(this, events, modifiers);
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>... events) throws IOException {
		return register(watcher, events, EMPTY_MODIFIERS_ARRAY);
	}

	@Override
	public Iterator<Path> iterator() {
		return new CloudPathIterator(this);
	}

	@Override
	public int compareTo(Path other) {
		if (this == other) {
			return 0;
		}
		
		if (!(other instanceof CloudPath)) {
			throw new ClassCastException("Cannot compare a path of type " + other.getClass().getName() +
					" with a " + CloudPath.class.getName());
		}

		List<String> myAllPaths = getAllPaths();
		List<String> otherAllPaths = ((CloudPath)other).getAllPaths();

		// Compare each path element
		boolean compareLeastMyPaths = myAllPaths.size() < otherAllPaths.size();
		for (int i=0; i<(compareLeastMyPaths ? myAllPaths.size() : otherAllPaths.size()); i++) {
			int compareResult = myAllPaths.get(i).compareTo(otherAllPaths.get(i));
			if (compareResult != 0) {
				return compareResult;
			}
		}

		// If we got this far then the remaining one with more paths is greater, or the paths are all equal
		return myAllPaths.size() == otherAllPaths.size() ? 0 :
				compareLeastMyPaths ? -1 : 1;
	}
	
	@Override
	public boolean equals(Object obj) {
		return compareTo((Path)obj) == 0;
	}
	
	@Override
	public int hashCode() {
		HashCodeBuilder builder = new HashCodeBuilder();
		getAllPaths().forEach(p -> builder.append(p));
		return builder.toHashCode();
	}

	@Override
	public String toString() {
		return (isAbsolute ? PATH_SEPARATOR : "") +
				StringUtils.join(constructPath(deconstructedPath, 0, deconstructedPath.size()), PATH_SEPARATOR);
	}
	
	/**
	 * Determines whether native optimised operations can be used between the two cloud paths.
	 * @see CloudHostConfiguration#canOptimiseOperationsFor(CloudPath)
	 */
	public boolean canOptimiseOperationsBetween(CloudPath cloudPath) {
		return this.getFileSystem().getCloudHostConfiguration().canOptimiseOperationsFor(cloudPath);
	}


	
	class CloudPathIterator implements Iterator<Path> {
		private CloudPath cloudPath;
		private int startIndex = 1;

		public CloudPathIterator(CloudPath cloudPath) {
			this.cloudPath = cloudPath.toAbsolutePath();
		}

		@Override
		public boolean hasNext() {
			return startIndex <= cloudPath.getNameCount();
		}

		@Override
		public CloudPath next() {
			return cloudPath.getName(startIndex++);
		}
	}

}
