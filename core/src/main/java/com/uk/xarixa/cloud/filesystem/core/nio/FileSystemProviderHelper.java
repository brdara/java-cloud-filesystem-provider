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
import java.util.Set;

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
    	FileSystemProvider provider = dir.getFileSystem().provider();
		// System.out.println("Root: " + dir.getRoot());
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
		Iterator<Path> pathIter = dir.toAbsolutePath().iterator();
		
		while (pathIter.hasNext()) {
			Path path = pathIter.next();

			if (!Files.isDirectory(path)) {
				LOG.debug("Attempting to create directory '{}'", path);
	
		    	try {
					provider.createDirectory(path);
					LOG.debug("Created directory '{}'", path);
				} catch (IOException e) {
					LOG.error("Could not create directory at '{}'", path, e);
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
	public static int copyDirectoryBetweenProviders(Path sourcePath, Path destinationPath) {
		// Work out the new path at the destination
		Path newDestinationPathDir =
				destinationPath.getFileSystem().getPath(
						destinationPath.toAbsolutePath().toString(),
						sourcePath.getName(-1).toString());
		ensureDirectoriesExist(newDestinationPathDir);

		FileSystemProvider provider = sourcePath.getFileSystem().provider();

		// Directory listing
		DirectoryStream<Path> dirStream;
		try {
			dirStream = provider.newDirectoryStream(sourcePath, new Filter<Path>() {
				@Override
				public boolean accept(Path entry) throws IOException {
					return true;
				}
			});
		} catch (Exception e) {
			LOG.error("Could not list directories for path '{}", sourcePath, e);
			return 0;
		}

		// For each result in the dir, copy each file
		int pathsCounter = 0;
		Iterator<Path> paths = dirStream.iterator();
		while (paths.hasNext()) {
			Path nextPath = paths.next();

			if (Files.isDirectory(nextPath)) {
				LOG.debug("Non-recursive copy specified, skipping directory '" + nextPath + "'");
			} else {
				Path copyPath = destinationPath.getFileSystem().getPath(
						newDestinationPathDir.toString(),
						nextPath.getFileName().toString());
				if (copyFileUsingDestinationProvider(nextPath, copyPath) == 1) {
					pathsCounter++;
				}
			}
		}

		return pathsCounter;
	}

	/**
	 * Atempts to copy a single file from the source to the destination. Some providers throw a
	 * {@link ProviderMismatchException} when attempting to copy between different providers.
	 * 
	 * @param sourcePath
	 * @param destinationPath
	 * @return
	 */
	public static int copyFileUsingDestinationProvider(Path sourcePath, Path destinationPath) {
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

}
