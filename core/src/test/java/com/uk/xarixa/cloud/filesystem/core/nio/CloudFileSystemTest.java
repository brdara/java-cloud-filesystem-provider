package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collection;

import org.jclouds.blobstore.BlobStoreContext;
import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.powermock.reflect.internal.WhiteboxImpl;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudFileSystemTest {
	private FileSystemProvider provider;
	private CloudHostConfiguration cloudHostSettings;
	private BlobStoreContext blobStoreContext;
	private CloudFileSystem impl;

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};

	@Before
	public void setUp() {
		provider = context.mock(FileSystemProvider.class);
		cloudHostSettings = context.mock(CloudHostConfiguration.class);
		blobStoreContext = context.mock(BlobStoreContext.class);
		impl = new CloudFileSystem(provider, cloudHostSettings, blobStoreContext);
	}

	@Test
	public void testGetPathCreatesANonAbsolutePath() {
		Assert.assertEquals("/root", impl.getPath("root").toString());
		Assert.assertEquals("/root/subdir1", impl.getPath("root", "subdir1").toString());
		Assert.assertEquals("/root/subdir1/subdir2", impl.getPath("root", "subdir1", "subdir2").toString());
	}
	
	@Test
	public void testGetUserPrincipalLookupServiceThrowsAnExceptionWhenTheServiceIsNull() {
		context.checking(new Expectations() {{
			allowing(cloudHostSettings).getUserPrincipalLookupService();
			will(returnValue(null));
		}});
		
		try {
			impl.getUserPrincipalLookupService();
			Assert.fail("Expected an exception to be thrown");
		} catch (UnsupportedOperationException e) {
			// OK
		}
	}

	@Test
	public void testGetUserPrincipalLookupServiceWillReturnTheServiceIfNotNull() {
		UserPrincipalLookupService service = context.mock(UserPrincipalLookupService.class);
		
		context.checking(new Expectations() {{
			allowing(cloudHostSettings).getUserPrincipalLookupService();
			will(returnValue(service));
		}});
		
		Assert.assertEquals(service, impl.getUserPrincipalLookupService());
	}

	@Test
	public void testNewWatchServiceThrowsAnExceptionWhenTheServiceIsNull() throws IOException {
		context.checking(new Expectations() {{
			allowing(cloudHostSettings).createWatchService();
			will(returnValue(null));
		}});
		
		try {
			impl.newWatchService();
			Assert.fail("Expected an exception to be thrown");
		} catch (UnsupportedOperationException e) {
			// OK
		}
	}

	@Test
	public void testNewWatchServiceWillReturnTheServiceIfNotNull() throws IOException {
		CloudWatchService service = context.mock(CloudWatchService.class);

		context.checking(new Expectations() {{
			allowing(cloudHostSettings).createWatchService();
			will(returnValue(service));
		}});

		Assert.assertEquals(service, impl.newWatchService());
	}
	
	@Test
	public void testNewWatchServiceWillTrackTheServiceAndCloseWillCloseAllWatchers() throws IOException {
		CloudWatchService service = context.mock(CloudWatchService.class, "service1");
		CloudWatchService service2 = context.mock(CloudWatchService.class, "service2");
		Sequence sequence = context.sequence("watch-service-sequence");

		context.checking(new Expectations() {{
			allowing(cloudHostSettings).getName();
			will(returnValue("unit-test"));
			
			exactly(1).of(cloudHostSettings).createWatchService();
			will(returnValue(service));
			inSequence(sequence);

			exactly(1).of(cloudHostSettings).createWatchService();
			will(returnValue(service2));
			inSequence(sequence);
			
			exactly(1).of(blobStoreContext).close();
			
			exactly(1).of(service).close();
			exactly(1).of(service2).close();
		}});

		Assert.assertEquals(service, impl.newWatchService());
		Assert.assertEquals(service2, impl.newWatchService());
		Assert.assertEquals(2, ((Collection<?>)WhiteboxImpl.getInternalState(impl, "cloudWatchServices")).size());
		impl.close();
		Assert.assertEquals(0, ((Collection<?>)WhiteboxImpl.getInternalState(impl, "cloudWatchServices")).size());
	}

	@Test
	public void testCloseWillThrowAnExceptionForSubsequentCalls() throws IOException {
		context.checking(new Expectations() {{
			allowing(cloudHostSettings).getName();
			will(returnValue("unit-test"));
			
			exactly(1).of(blobStoreContext).close();
		}});

		impl.close();
		Assert.assertFalse(impl.isOpen());
		
		try {
			impl.getBlobStoreContext();
			Assert.fail("Expected an exception");
		} catch (ClosedFileSystemException e) {
			// OK
		}
	}

}
