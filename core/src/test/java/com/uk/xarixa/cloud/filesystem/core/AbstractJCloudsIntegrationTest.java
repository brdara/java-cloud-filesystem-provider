package com.uk.xarixa.cloud.filesystem.core;

import java.io.IOException;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.domain.Location;
import org.jclouds.openstack.swift.SwiftApiMetadata;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import com.google.common.collect.Iterables;
import com.google.common.net.MediaType;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProviderDelegate;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;

/**
 * @see CloudFileSystemLiveTestHelper
 */
public abstract class AbstractJCloudsIntegrationTest {
	public final static String CONTAINER_NAME = "com.uk.xarixa.cloud.filesystem.nio-test-container-root";
	protected BlobStoreContext blobStoreContext;
	protected CloudFileSystem fileSystem;
	protected FileSystemProvider provider;
	protected Location location;
	protected CloudHostConfiguration cloudHostSettings;
	protected CloudPath containerPath;
	private final AtomicInteger cloudPathCounter = new AtomicInteger(0);

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};


	/**
	 * <ul>
	 * <li>Creates a {@link #blobStoreContext} using the {@link CloudFileSystemLiveTestHelper} class.
	 * <li>Creates the {@link #CONTAINER_NAME container}.
	 * </ul>
	 */
	@Before
	public final void setUp() {
		preSetUp();
		cloudPathCounter.set(0);
		cloudHostSettings = CloudFileSystemLiveTestHelper.getCloudHostSettings();
		blobStoreContext = CloudFileSystemLiveTestHelper.createBlobStoreContext(cloudHostSettings);
		fileSystem = context.mock(CloudFileSystem.class);
		provider = new CloudFileSystemProviderDelegate();

		// If Swift get a location
		ApiMetadata apiMetadata = blobStoreContext.unwrap().getProviderMetadata().getApiMetadata();
		location = null;
        if (apiMetadata instanceof SwiftApiMetadata) {
            location = Iterables.getFirst(blobStoreContext.getBlobStore().listAssignableLocations(), null);
        }

		if (!blobStoreContext.getBlobStore().createContainerInLocation(location, CONTAINER_NAME)) {
			blobStoreContext.getBlobStore().deleteContainer(CONTAINER_NAME);
			Assert.assertTrue("Could not create container " + CONTAINER_NAME,
					blobStoreContext.getBlobStore().createContainerInLocation(location, CONTAINER_NAME));
		}
        
        context.checking(new Expectations() {{
        	allowing(fileSystem).provider();
        	will(returnValue(provider));
        	
        	allowing(fileSystem).getCloudHostConfiguration();
        	will(returnValue(cloudHostSettings));
        	
        	allowing(fileSystem).getBlobStoreContext();
        	will(returnValue(blobStoreContext));
        }});
        
		containerPath = new CloudPath(fileSystem, true, CONTAINER_NAME);

        postSetUp();
	}
	
	protected void postSetUp() {
	}

	protected void preSetUp() {
	}

	@After
	public final void tearDown() {
		preTearDown();
		blobStoreContext.getBlobStore().deleteContainer(CONTAINER_NAME);
		postTearDown();
	}
	
	protected void postTearDown() {
	}

	protected void preTearDown() {
	}

	/**
	 * Directly uses the {@link BlobStore} to build a BLOB and create content
	 * @param contentPath
	 * @param content
	 */
	public final void createRawContent(String contentPath, byte[] content) {
		createRawContent(contentPath, null, content);
	}

	/**
	 * As {@link #createRawContent(String, byte[])} except sets {@link BlobAccess} for the path also
	 * @param contentPath
	 * @param access
	 * @param content
	 */
	public final void createRawContent(String contentPath, BlobAccess access, byte[] content) {
//		contentPaths.add(contentPath);
		BlobStore blobStore = blobStoreContext.getBlobStore();
		Blob blob = blobStore.blobBuilder(contentPath)
			    .payload(content)
			    .contentLength(content.length)
			    .contentType(MediaType.OCTET_STREAM.toString())
			    .build();
		blobStore.putBlob(CONTAINER_NAME, blob);
		
		if (access != null) {
			blobStore.setBlobAccess(CONTAINER_NAME, contentPath, access);
		}
	}
	

	public final String directoryContentsToString(String path) {
		BlobStore blobStore = blobStoreContext.getBlobStore();
		String marker = null;
		ListContainerOptions opts = new ListContainerOptions().recursive();
		if (StringUtils.isNotBlank(path)) {
			opts.inDirectory(path);
		}

		StringBuilder ret = new StringBuilder((path == null ? "Container '" : "Directory '" + path)).append("' contains: [ ");
		do {
			if (marker != null) {
				opts.afterMarker(marker);
			}

			PageSet<? extends StorageMetadata> page = blobStore.list(CONTAINER_NAME, opts);
			page.forEach(
					m -> ret.append('{').append(m.getName()).append(" (")
							.append(m.getType() == StorageType.BLOB ? "FILE" : "DIRECTORY").append(")} "));
			marker = page.getNextMarker();
		} while (marker != null);
		
		return ret.append(']').toString();
	}

	/**
	 * Directly retrieves content
	 * @param contentPath
	 * @return
	 * @throws IOException
	 */
	public final List<String> getContentAsLines(String contentPath) throws IOException {
		BlobStore blobStore = blobStoreContext.getBlobStore();
		Blob blob = blobStore.getBlob(CONTAINER_NAME, contentPath);
		return IOUtils.readLines(blob.getPayload().openStream());
	}

}
