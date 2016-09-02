package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.ProviderNotFoundException;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is to get around the limitation with installed providers in {@link FileSystems}. When
 * {@link #newFileSystem(URI, Map, ClassLoader)} is invoked then the {@link FileSystemProvider} is
 * saved so that it can be used again for any matching scheme when {@link #getFileSystem(URI)} is
 * invoked.
 */
public class FileSystemProviderHelper {
	private final static Set<WeakReference<FileSystemProvider>> providers = new HashSet<>();

	public static FileSystem getFileSystem(URI uri) {
		try {
			return FileSystems.getFileSystem(uri);
		} catch (FileSystemNotFoundException | ProviderNotFoundException e) {
		}

		for (WeakReference<FileSystemProvider> providerRef : providers) {
			FileSystemProvider provider = providerRef.get();
			
			if (provider != null && uri.getScheme().equals(provider.getScheme())) {
				return provider.getFileSystem(uri);
			}
		}
		
		throw new ProviderNotFoundException("Could not find provider for scheme '" + uri.getScheme() + "'");
	}
	
	public static FileSystem newFileSystem(URI uri, Map<String,?> env, ClassLoader loader) throws IOException {
		FileSystem fileSystem = FileSystems.newFileSystem(uri, env, loader);
		
		if (fileSystem != null) {
			FileSystemProvider provider = fileSystem.provider();
			providers.add(new WeakReference<FileSystemProvider>(provider));
		}
		
		return fileSystem;
	}

}
