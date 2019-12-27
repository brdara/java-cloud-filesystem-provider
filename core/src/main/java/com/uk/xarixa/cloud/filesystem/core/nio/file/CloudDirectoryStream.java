package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPathWithAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;

public class CloudDirectoryStream implements DirectoryStream<CloudPath> {
	private final ListContainerOptions listContainerOptions;
	private final CloudPath dirPath;
	private final boolean isContainer;
	private final boolean isRecursive;
	private final DirectoryStream.Filter<CloudPath> filter;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final String dirPathName;

	/**
	 * Lists the container in a non-recursive manner: <em>this(dirPath, isContainer, false, filter)</em>
	 * @param dirPath
	 * @param isContainer
	 * @param filter
	 */
	CloudDirectoryStream(CloudPath dirPath, boolean isContainer, DirectoryStream.Filter<CloudPath> filter) {
		this(dirPath, isContainer, false, filter);
	}

	/**
	 * 
	 * @param dirPath		The path
	 * @param isContainer	Whether this is a top-level container or not
	 * @param isRecursive	Whether this is a recursive listing. If false
	 * @param filter
	 */
	public CloudDirectoryStream(CloudPath dirPath, boolean isContainer, boolean isRecursive, DirectoryStream.Filter<CloudPath> filter) {
		this.dirPath = dirPath;
		this.isContainer = isContainer;
		this.isRecursive = isRecursive;
		this.filter = filter;
		this.listContainerOptions = new ListContainerOptions();
		this.dirPathName = dirPath.getPathName();

		if (!isContainer) {
			listContainerOptions.inDirectory(dirPath.getPathName());
		}

		if (isRecursive) {
			listContainerOptions.recursive();
		}
	}

	@Override
	public void close() throws IOException {
		closed.set(true);
	}

	@Override
	public Iterator<CloudPath> iterator() {
		if (closed.get()) {
			throw new IllegalStateException("This directory stream has already been closed");
		}
		
		return new DirectoryStreamIterator();
	}

	public boolean isContainer() {
		return isContainer;
	}

	public boolean isRecursive() {
		return isRecursive;
	}

	
	class DirectoryStreamIterator implements Iterator<CloudPath> {
		private boolean readNextPage = true;
		private String pageSetMarker = null;
		private Iterator<CloudPath> currentPage = null;

		void ensureRead() {
			if (readNextPage && (currentPage == null || !currentPage.hasNext())) {
				try {
					readNextListing();
				} catch (IOException e) {
					throw new RuntimeException("Cannot read file listing", e);
				}
			}
		}

		void readNextListing() throws IOException {
			BlobStore blobStore = dirPath.getFileSystem().getBlobStoreContext().getBlobStore();
			List<CloudPath> paths = new ArrayList<>();

			// Set next page marker
			if (pageSetMarker != null) {
				listContainerOptions.afterMarker(pageSetMarker);
			}

			// Perform a file listing
			PageSet<? extends StorageMetadata> pageSet =
					blobStore.list(dirPath.getContainerName(), listContainerOptions);

			for (StorageMetadata meta : pageSet) {
				String filename = dirPathName == null ? meta.getName() :
					StringUtils.substringAfter(meta.getName(), dirPathName);

				// The listing returns the directory name as part of the listing, don't return this
				if (StringUtils.isNotBlank(filename) && !StringUtils.equals(filename, CloudPath.DEFAULT_PATH_SEPARATOR)) {
					CloudBasicFileAttributes cloudFileAttributes = new CloudBasicFileAttributes(meta);
					CloudPath path =
							new CloudPathWithAttributes(dirPath.getFileSystem(), false, dirPath, filename, cloudFileAttributes);
	
					if (filter == null || filter.accept(path)) {
						paths.add(path);
					}
				}
			}

			currentPage = paths.iterator();
			pageSetMarker = pageSet.getNextMarker();
			readNextPage = pageSetMarker != null;
		}

		@Override
		public boolean hasNext() {
			ensureRead();
			return currentPage != null && currentPage.hasNext();
		}

		@Override
		public CloudPath next() {
			ensureRead();
			return currentPage.next();
		}
		
	}

}
