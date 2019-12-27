package com.uk.xarixa.cloud.filesystem.core.host.factory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;

import org.jclouds.blobstore.BlobStoreContext;
import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.powermock.reflect.internal.WhiteboxImpl;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.MockCloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;

@RunWith(BlockJUnit4ClassRunner.class)
public class JCloudsCloudHostProviderTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};
	private JCloudsCloudHostProvider impl;
	
	@Before
	public void setUp() {
		impl = new JCloudsCloudHostProvider();
	}
	
	@Test
	public void testCreateCloudFileSystemInternalWillCreateTheFileSystemIfItHasntBeenCreatedYet() {
		CloudHostConfiguration config = context.mock(CloudHostConfiguration.class);
		FileSystemProvider provider = context.mock(FileSystemProvider.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);

		context.checking(new Expectations() {{
			allowing(config).getName();
			will(returnValue("test-config"));
			
			exactly(1).of(config).createBlobStoreContext();
			will(returnValue(blobStoreContext));
		}});

		Assert.assertTrue(((Map<?,?>)WhiteboxImpl.getInternalState(impl, "cloudFileSystems")).isEmpty());
		impl.createCloudFilesystemInternal(provider, config);
		Map<String,CloudFileSystem> cloudFileSystemsMap =
				((Map<String,CloudFileSystem>)WhiteboxImpl.getInternalState(impl, "cloudFileSystems"));
		Assert.assertTrue(cloudFileSystemsMap.containsKey("test-config"));
		Assert.assertNotNull(cloudFileSystemsMap.get("test-config"));
	}

	@Test
	public void testCreateCloudFileSystemInternalWillThrowAnErrorIfAnAttemptIsMadeToCreateTheFilesystemMoreThanOnce() {
		CloudHostConfiguration config = context.mock(CloudHostConfiguration.class);
		FileSystemProvider provider = context.mock(FileSystemProvider.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);

		context.checking(new Expectations() {{
			allowing(config).getName();
			will(returnValue("test-config"));
			
			exactly(1).of(config).createBlobStoreContext();
			will(returnValue(blobStoreContext));
		}});

		impl.createCloudFilesystemInternal(provider, config);
		
		try {
			impl.createCloudFilesystemInternal(provider, config);
			Assert.fail("Expected an exception");
		} catch (DuplicateCloudHostnameException e) {
			// OK
		}
	}

	@Test
	public void testCreateCloudFileSystemInternalWillAllowTheFileSystemToBeCreatedAgainAfterItHasBeenClosed() throws IOException {
		CloudHostConfiguration config = context.mock(CloudHostConfiguration.class);
		FileSystemProvider provider = context.mock(FileSystemProvider.class);
		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class);

		context.checking(new Expectations() {{
			allowing(config).getName();
			will(returnValue("test-config"));
			
			exactly(2).of(config).createBlobStoreContext();
			will(returnValue(blobStoreContext));
			
			exactly(1).of(blobStoreContext).close();
		}});

		// Create and close
		CloudFileSystem fs = impl.createCloudFilesystemInternal(provider, config);
		fs.close();
		
		// Should now be able to create again
		CloudFileSystem fs2 = impl.createCloudFilesystemInternal(provider, config);
		Assert.assertNotEquals(fs, fs2);
	}

	@Test
	public void testCreateCloudFileSystemWillUseTheCloudHostConfigurationBuilderToCreateACloudFileSystem() throws URISyntaxException, IOException {
		FileSystemProvider provider = context.mock(FileSystemProvider.class);
		URI uri = new URI("cloud", "mock-fs", "/path", "fragment"); // The host holds the name
		Map<String,Object> env = new HashMap<>();
		env.put(JCloudsCloudHostProvider.CLOUD_TYPE_ENV, "mock-test");

		// Test we can create the FS
		Assert.assertTrue(((Map<?,?>)WhiteboxImpl.getInternalState(impl, "cloudFileSystems")).isEmpty());
		impl.createCloudFileSystem(provider, uri, env);
		Map<String,CloudFileSystem> cloudFileSystemsMap =
				((Map<String,CloudFileSystem>)WhiteboxImpl.getInternalState(impl, "cloudFileSystems"));
		Assert.assertTrue(cloudFileSystemsMap.containsKey("mock-fs"));
		Assert.assertNotNull(cloudFileSystemsMap.get("mock-fs"));

		// Now get the FS back
		CloudFileSystem cloudFileSystem = impl.getCloudFileSystem(uri);
		Assert.assertNotNull(cloudFileSystem);
		Assert.assertEquals(provider, cloudFileSystem.provider());
		Assert.assertEquals(MockCloudHostConfiguration.class, cloudFileSystem.getCloudHostConfiguration().getClass());
		Assert.assertEquals("mock-fs", cloudFileSystem.getCloudHostConfiguration().getName());
		
		// Close it and make sure we don't get it back
		cloudFileSystem.close();
		Assert.assertNull(impl.getCloudFileSystem(uri));
	}

}
