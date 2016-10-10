package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.ProviderNotFoundException;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is to get around the limitation with installed providers in {@link FileSystems}. When
 * {@link #newFileSystem(URI, Map, ClassLoader)} is invoked then the {@link FileSystemProvider} is
 * saved so that it can be used again for any matching scheme when {@link #getFileSystem(URI)} is
 * invoked.
 */
public final class FileSystemProviderHelper {
	private static final Logger LOG = LoggerFactory.getLogger(FileSystemProviderHelper.class);
	private final static Set<WeakReference<FileSystemProvider>> providers = new HashSet<>();
	
	/**
	 * A {@link Filter} which accepts any path
	 */
	public static class AcceptAllFilter implements Filter<Path> {
		@Override
		public boolean accept(Path entry) throws IOException {
			return true;
		}
	};

	private FileSystemProviderHelper() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Retrieves a file system using the default {@link FileSystems#getFileSystem(URI)}. If this
	 * throws a 
	 * @param uri
	 * @return
	 */
	public static FileSystem getFileSystem(URI uri) {
		try {
			return FileSystems.getFileSystem(uri);
		} catch (FileSystemNotFoundException | ProviderNotFoundException e) {
			LOG.debug("File system scheme " + uri.getScheme() +
					" not found in the default installed providers list, attempting to find this in the "
					+ "list of additional providers");
		}

		for (WeakReference<FileSystemProvider> providerRef : providers) {
			FileSystemProvider provider = providerRef.get();
			
			if (provider != null && uri.getScheme().equals(provider.getScheme())) {
				return provider.getFileSystem(uri);
			}
		}
		
		throw new ProviderNotFoundException("Could not find provider for scheme '" + uri.getScheme() + "'");
	}
	
	/**
	 * Invoke this method instead of {@link FileSystems#newFileSystem(URI, Map, ClassLoader)} which
	 * saves the provider for use by {@link #getFileSystem(URI)}
	 * 
	 * @param uri
	 * @param env
	 * @param loader
	 * @return
	 * @throws IOException
	 */
	public static FileSystem newFileSystem(URI uri, Map<String,?> env, ClassLoader loader) throws IOException {
		FileSystem fileSystem = FileSystems.newFileSystem(uri, env, loader);
		
		if (fileSystem != null) {
			FileSystemProvider provider = fileSystem.provider();
			providers.add(new WeakReference<FileSystemProvider>(provider));
		}
		
		return fileSystem;
	}

	/**
	 * Ensures that all parts of the path exist as directories. If an error occurs it is logged.
	 * @param dir
	 * @return true if the action completed otherwise false
	 */
	public static boolean ensureDirectoriesExist(Path dir) {
		LOG.debug("Ensuring directories exist for {}", dir);
    	FileSystemProvider provider = dir.getFileSystem().provider();
		Path rootContainer = dir.getRoot();

		if (!Files.exists(rootContainer)) {
			LOG.debug("Attempting to create root container '{}'", rootContainer);

			try {
				provider.createDirectory(rootContainer);
				LOG.debug("Created directory '{}'", rootContainer);
			} catch (IOException e) {
				LOG.error("Could not create directory at '{}'", rootContainer, e);
				return false;
			}
		}

		// Ensure container exists
		Iterator<Path> pathIter = dir.iterator();
		Path currentPath = rootContainer;
		FileSystem fileSystem = dir.getFileSystem();

		while (pathIter.hasNext()) {
			// Path iterator gives us a part of the path, we have to build it up
			Path relativePath = pathIter.next();
			currentPath = fileSystem.getPath(currentPath.toString() +
					fileSystem.getSeparator() + relativePath.getFileName().toString());

			if (!Files.isDirectory(currentPath)) {
				LOG.debug("Attempting to create directory '{}'", currentPath);

		    	try {
					provider.createDirectory(currentPath);
					LOG.debug("Created directory '{}'", currentPath);
				} catch (IOException e) {
					LOG.error("Could not create directory at '{}'", currentPath, e);
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Atempts to copy a directory from the source to the destination. Some providers throw a
	 * {@link ProviderMismatchException} when attempting to copy between different providers.
	 * 
	 * @param sourcePath
	 * @param destinationPath
	 * @return
	 */
	public static int copyDirectoryBetweenProviders(Path sourcePath, Path destinationPath, Filter<Path> pathFilter,
			boolean recursive) {
		// Work out the new path at the destination and make sure the directories exist
		final String sourceFsSeparator = sourcePath.getFileSystem().getSeparator();
		final FileSystem destinationFileSystem = destinationPath.getFileSystem();
		final String destinationFsSeparator = destinationFileSystem.getSeparator();
		StringBuilder newDestinationPathString = new StringBuilder(destinationPath.toAbsolutePath().toString())
				.append(destinationFsSeparator)
				.append(sourcePath.getFileName().toString());
		Path newDestinationPathDir =
				destinationFileSystem.getPath(newDestinationPathString.toString());
		ensureDirectoriesExist(newDestinationPathDir);

		AtomicInteger pathsCopied = new AtomicInteger(0);
		iterateOverDirectoryContents(sourcePath.getFileSystem(), Optional.ofNullable(sourcePath), pathFilter,
				recursive, subPath -> {
					Optional<Path> relativeSourcePath = subPath.getRelativeSourcePath();
					StringBuilder destinationPathString = new StringBuilder(newDestinationPathDir.toString())
							.append(destinationFsSeparator);
					
					// Append the relative path if it exists
					if (relativeSourcePath.isPresent()) {
						if (sourceFsSeparator.equals(destinationFsSeparator)) {
							destinationPathString.append(relativeSourcePath.get().toString())
								.append(destinationFsSeparator);
						} else {
							destinationPathString.append(
									StringUtils.replace(relativeSourcePath.get().toString(),
											sourceFsSeparator, destinationFsSeparator))
								.append(destinationFsSeparator);
						}
					}

					destinationPathString.append(subPath.getResultPath().getFileName());
					Path copyPath = destinationFileSystem.getPath(destinationPathString.toString());
					LOG.debug("Created destination path {} (dest string={}, root={}, relative={}, name={})",
							copyPath,
							destinationPathString,
							newDestinationPathDir,
							relativeSourcePath.isPresent() ? relativeSourcePath.get() : "N/A",
							subPath.getResultPath().getFileName());

					if (Files.isDirectory(subPath.getResultPath())) {
						if (recursive) {
							// Create the dir
							ensureDirectoriesExist(copyPath);
						}
					} else {
						// Copy the content
						if (copyFileBetweenProviders(subPath.getResultPath(), copyPath) == 1) {
							pathsCopied.incrementAndGet();
						}
					}

					return true;
				});

		return pathsCopied.get();
	}

	/**
	 * Atempts to copy a single file from the source to the destination. Some providers throw a
	 * {@link ProviderMismatchException} when attempting to copy between different providers.
	 * 
	 * @param sourcePath
	 * @param destinationPath
	 * @return
	 */
	public static int copyFileBetweenProviders(Path sourcePath, Path destinationPath) {
		LOG.debug("Copying source file {} -> {}", sourcePath, destinationPath);
		
		// Get the providers
		FileSystemProvider destinationProvider = destinationPath.getFileSystem().provider();
		long readSize = 0L;

		// Open source file for reading
		try (FileChannel readChannel = FileChannel.open(sourcePath,
				EnumSet.of(StandardOpenOption.READ))) {
			readSize = readChannel.size();
			
			// Open destination for writing
			try (SeekableByteChannel targetChannel = destinationProvider.newByteChannel(destinationPath,
						EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
				readChannel.transferTo(0L, readSize, targetChannel);
			} catch (IOException e) {
				LOG.error("Error writing to destination '{}'", destinationPath, e);
				return 0;
			}
		} catch (IOException e) {
			LOG.error("Error reading from source '{}'", sourcePath);
			return 0;
		}
		
		LOG.debug("Transferred " + readSize + " bytes from '" + sourcePath + "' to '" + destinationPath + "'");
		return 1;
	}

	/**
	 * Iterates across a directory's contents
	 * @param path				An optional path. If the option is empty then iterate's over
	 * 							{@link FileSystem#getRootDirectories() root directories}
	 * @param pathFilter		Filter used in the {@link FileSystemProvider#newDirectoryStream(Path, Filter)} call
	 * @param recursiveListing	Whether to follow directories recursively
	 * @param directoryContentAction	A function which consumes the path and returns a boolean, indicating whether traversal
	 * 									should continue or not.
	 * @return true if traversal completed, false if this was cancelled.
	 */
	public static boolean iterateOverDirectoryContents(FileSystem fileSystem, Optional<Path> path, Filter<Path> pathFilter,
			boolean recursiveListing, Function<DirectoryIterationPaths,Boolean> directoryContentAction) {
		// If there is no path then provide a listing for the root directories
		if (!path.isPresent()) {
			Iterable<Path> rootDirectories = fileSystem.getRootDirectories();
			for (Path rootDir : rootDirectories) {
				DirectoryIterationPaths iterationPaths = new DirectoryIterationPaths(rootDir, null, rootDir);
				if (!directoryContentAction.apply(iterationPaths)) {
					return false;
				}

				// Recurse through subdirs as required
				if (recursiveListing && Files.isDirectory(rootDir)) {
					if (!iterateOverDirectoryContents(fileSystem, iterationPaths, pathFilter,
							recursiveListing, directoryContentAction)) {
						return false;
					}
				}
			}
			
			//All successful
			return true;
		} else {
			DirectoryIterationPaths iterationPaths = new DirectoryIterationPaths(path.get(), null, path.get());
			return iterateOverDirectoryContents(fileSystem, iterationPaths, pathFilter, recursiveListing,
					directoryContentAction);
		}
	}
	
	private static boolean iterateOverDirectoryContents(FileSystem fileSystem, DirectoryIterationPaths dirIterationPaths,
			Filter<Path> pathFilter, boolean recursiveListing, Function<DirectoryIterationPaths,Boolean> directoryContentAction) {
		Path path = dirIterationPaths.getResultPath();
		boolean printRecursive;
		FileSystemProvider provider = fileSystem.provider();
		DirectoryStream<Path> dirStream;

		// Directory listing
		try {
			
			if (recursiveListing && provider instanceof CloudFileSystemProvider) {
				// The CloudFileSystemProvider type is capable of providing a recursive listing,
				// use this for efficiency if possible
				LOG.debug("Using optimised cloud file system provider for recursive file listing");
				dirStream = ((CloudFileSystemProvider)provider).newDirectoryStream(path, pathFilter, true);
				printRecursive = false;
			} else {
				// Fallback to using recursive printing of the file listing when a directory is encountered
				dirStream = provider.newDirectoryStream(path, pathFilter);
				printRecursive = recursiveListing;
			}
		} catch (Exception e) {
			LOG.error("Could not list directories for path '{}'", path, e);
			return false;
		}

		// For each result in the dir 
		Iterator<Path> paths = dirStream.iterator();
		while (paths.hasNext()) {
			Path nextPath = paths.next();
			DirectoryIterationPaths nextDirIterationPaths =
					new DirectoryIterationPaths(dirIterationPaths, nextPath);
			LOG.debug("Iterating through next path {}", nextDirIterationPaths);
			
			// Can throw CancelException to terminate recursion/listing
			if (!directoryContentAction.apply(nextDirIterationPaths)) {
				return false;
			}

			// Recurse through subdirs as required
			if (printRecursive && Files.isDirectory(nextPath)) {
				if (!iterateOverDirectoryContents(fileSystem, nextDirIterationPaths, pathFilter,
						printRecursive, directoryContentAction)) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	/**
	 * Parameter passed into the function which is passed into
	 * {@link FileSystemProviderHelper#iterateOverDirectoryContents(FileSystem, Optional, Filter, boolean, Function)}.
	 * <ul>
	 * <li><em>iterationStartPath</em> is the path which the iterative process started from, i.e. the first
	 * invocation of {@link FileSystemProviderHelper#iterateOverDirectoryContents(FileSystem, Optional, Filter, boolean, Function)}.</li>
	 * <li><em>sourcePath</em> is the container or directory which was traversed and contains the <em>resultPath</em>.</li>
	 * <li><em>resultPath</em> is the path to iterate over.</li>
	 * </ul>
	 */
	public static final class DirectoryIterationPaths {
		private final Path iterationStartPath;
		private final Path sourcePath;
		private final Path resultPath;

		public DirectoryIterationPaths(DirectoryIterationPaths sourceDirectoryIterationPaths, Path resultPath) {
			this(sourceDirectoryIterationPaths.iterationStartPath,
					sourceDirectoryIterationPaths.resultPath, resultPath);
			LOG.debug("Created {} from {}, {}", this, sourceDirectoryIterationPaths, resultPath);
		}

		public DirectoryIterationPaths(Path iterationStartPath, Path sourcePath, Path resultPath) {
			this.iterationStartPath = iterationStartPath;
			this.sourcePath = sourcePath;
			this.resultPath = resultPath;
			LOG.debug("Created {} from {}, {}, {}", this, iterationStartPath, sourcePath, resultPath);
		}
		
		public Path getIterationStartPath() {
			return iterationStartPath;
		}

		public Path getSourcePath() {
			return sourcePath;
		}

		public Path getResultPath() {
			return resultPath;
		}

		/**
		 * This gives the relative source path from the <em>iterationStartPath</em> to the <em>sourcePath</em>
		 * in the <em>targetFs</em>'s filesystem.
		 * For example if the <em>iterationStartPath</em> is <strong>/root/start</strong> and the <em>sourcePath</em> is
		 * <strong>/root/start/source</strong> then the relative path result is <em>start/source</em>.
		 * This is useful for things such as copy operations where you wish to know the relative path of this iteration.
		 * 
		 * @return A null-valued {@link Optional} with not value if there is not relative subpath or an {@link Optional}
		 */
		public Optional<Path> getRelativeSourcePath() {
			int startPathRelativeIndex = iterationStartPath == null ? 0 : iterationStartPath.getNameCount();

			// There is no difference
			if (sourcePath.getNameCount() == startPathRelativeIndex) {
				return Optional.empty();
			}

			String startPathName = sourcePath.toAbsolutePath()
						.subpath(startPathRelativeIndex, sourcePath.getNameCount()).toString();
			
			return Optional.of(resultPath.getFileSystem().getPath(startPathName));
		}
		
		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this);
		}
	}

}
