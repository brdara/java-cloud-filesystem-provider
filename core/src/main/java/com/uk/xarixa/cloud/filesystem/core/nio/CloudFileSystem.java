package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;
import com.uk.xarixa.cloud.filesystem.core.utils.DefaultPathMatcher;

public class CloudFileSystem extends FileSystem {
	private final static Logger LOG = LoggerFactory.getLogger(CloudFileSystem.class);
	private final static Set<String> supportedFileAttributeViews = Sets.newHashSet("basic", CloudFileAttributesView.VIEW_NAME);
	private final CloudHostConfiguration config;
	private final BlobStoreContext context;
	private final List<FileStore> fileStores = new ArrayList<>(1);
	private AtomicBoolean closed = new AtomicBoolean(false);
	private FileSystemProvider provider;
	private Set<WeakReference<CloudWatchService>> cloudWatchServices =
			new ConcurrentSkipListSet<>(new Comparator<WeakReference<CloudWatchService>>() {
				@Override
				public int compare(WeakReference<CloudWatchService> o1, WeakReference<CloudWatchService> o2) {
					// We don't really care what we return here to be honest, we need the
					// comparator because WeakReference doesn't implement Comparable
					return -1;
				}
			});

	public CloudFileSystem(FileSystemProvider provider, CloudHostConfiguration config, BlobStoreContext context) {
		this.provider = provider;
		this.config = config;
		this.context = context;
		fileStores.add(new CloudFileStore(config));
	}
	
	public CloudHostConfiguration getCloudHostConfiguration() {
		checkClosed();
		return config;
	}

	@Override
	public FileSystemProvider provider() {
		checkClosed();
		return provider;
	}

	@Override
	public void close() throws IOException {
		if (!closed.getAndSet(true)) {
			LOG.info("Closing filesystem '{}'", config.getName());
			IOUtils.closeQuietly(context);
			for (Iterator<WeakReference<CloudWatchService>> refIter = cloudWatchServices.iterator(); refIter.hasNext();) {
				try {
					WeakReference<CloudWatchService> ref = refIter.next();

					if (ref.get() != null) {
						IOUtils.closeQuietly(ref.get());
					}
				} catch (Exception e) {
					LOG.warn("There was an exception closing the cloud watch services for filesystem '{}'", config.getName());
				}
			}
			cloudWatchServices.clear();
			LOG.info("Closed filesystem '{}'", config.getName());
		}
	}
	
	void checkClosed() throws ClosedFileSystemException {
		if (closed.get()) {
			throw new ClosedFileSystemException();
		}
	}

	@Override
	public boolean isOpen() {
		return !closed.get();
	}

	/**
	 * Always false in this implementation
	 */
	@Override
	public boolean isReadOnly() {
		// TODO: Objey startup parameters for RO filesystem
		return false;
	}

	@Override
	public String getSeparator() {
		return CloudPath.PATH_SEPARATOR;
	}

	/**
	 * This lists all accessible containers
	 */
	@Override
	public Iterable<Path> getRootDirectories() {
		checkClosed();
		PageSet<? extends StorageMetadata> list = context.getBlobStore().list();
		List<Path> paths = new ArrayList<>();
		list.stream().forEach(m -> paths.add(new CloudPath(this, true, m.getName())));
		return paths;
	}

	/**
	 * This will always return a single file store as the notion of volumes is essentially the containers
	 * in the Cloud, and so is addressed as the root part of the {@link CloudPath}
	 * @return
	 */
	@Override
	public Iterable<FileStore> getFileStores() {
		checkClosed();
		return Collections.unmodifiableCollection(fileStores);
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		return supportedFileAttributeViews;
	}

	@Override
	public CloudPath getPath(String first, String... more) {
		checkClosed();
		List<String> pathArray = new ArrayList<>();
		pathArray.add(StringUtils.strip(first, "/"));
		Arrays.stream(more).forEach(p -> pathArray.add(StringUtils.strip(p, "/")));
		return new CloudPath(this, true, null, pathArray);
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		checkClosed();
		return new DefaultPathMatcher(syntaxAndPattern);
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		checkClosed();
		return Optional.ofNullable(config.getUserPrincipalLookupService())
				.orElseThrow(UnsupportedOperationException::new);
	}

	@Override
	public CloudWatchService newWatchService() throws IOException {
		checkClosed();
		CloudWatchService watchService = Optional.ofNullable(config.createWatchService())
				.orElseThrow(UnsupportedOperationException::new);
		trackWatchService(watchService);
		return watchService;
	}

	void trackWatchService(CloudWatchService watchService) {
		try {
			// Clean up old references in the list
			for (Iterator<WeakReference<CloudWatchService>> refIter = cloudWatchServices.iterator(); refIter.hasNext();) {
				WeakReference<CloudWatchService> ref = refIter.next();

				if (ref.get() == null) {
					refIter.remove();
				}
			}
		} finally {
			cloudWatchServices.add(new WeakReference<CloudWatchService>(watchService));
		}
	}

	/**
	 * Returns the underlying JClouds {@link BlobStoreContext}
	 * @return
	 */
	public BlobStoreContext getBlobStoreContext() {
		checkClosed();
		return context;
	}

}
