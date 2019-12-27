package com.uk.xarixa.cloud.filesystem.core.nio;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.nio.options.CloudCopyOption;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudFileSystemProviderDelegateTest {
	private CloudFileSystemProviderDelegate impl;
	private AtomicInteger cloudFileSystemImplementationExpectationsCounter;
	private AtomicInteger cloudPathExpectationsCounter;

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	@Before
	public void setUp() {
		impl = new CloudFileSystemProviderDelegate();
		cloudFileSystemImplementationExpectationsCounter = new AtomicInteger(1);
		cloudPathExpectationsCounter = new AtomicInteger(1);
	}

	@Test
	public void testGetSchemeReturnsTheCloudScheme() {
		Assert.assertEquals("cloud", impl.getScheme());
	}
	
	@Test
	public void testDeleteOptionsToEnumSetForAnEmptySetOfOptionsReturnsAnEmptyEnumSet() {
		EnumSet<DeleteOption> opts = impl.deleteOptionsToEnumSet();
		Assert.assertNotNull(opts);
		Assert.assertEquals(0, opts.size());
	}
	
	@Test
	public void testDeleteOptionsToEnumSetReturnsASetWithMultiplevalues() {
		EnumSet<DeleteOption> opts = impl.deleteOptionsToEnumSet(DeleteOption.FAIL_SILENTLY, DeleteOption.RECURSIVE);
		Assert.assertNotNull(opts);
		Assert.assertEquals(2, opts.size());
		Assert.assertTrue(opts.contains(DeleteOption.FAIL_SILENTLY));
		Assert.assertTrue(opts.contains(DeleteOption.RECURSIVE));
	}
	
	@Test
	public void testGetCloudFileSystemImplementationWillReturnTheImplementationFromTheCloudHostSettings() {
		CloudPath path = context.mock(CloudPath.class);
		CloudFileSystemImplementation cfsImpl = createGetCloudFileSystemImplementationExpectations(path);
		Assert.assertEquals(cfsImpl, impl.getCloudFileSystemImplementation(path));
	}
	
	private CloudFileSystemImplementation createGetCloudFileSystemImplementationExpectations(
			 CloudPath... paths) {
		CloudFileSystem fileSystem = context.mock(CloudFileSystem.class,
				"fileSystem-" + cloudFileSystemImplementationExpectationsCounter.getAndIncrement());
		return createGetCloudFileSystemImplementationExpectations(fileSystem, true, paths);
	}

	private CloudFileSystemImplementation createGetCloudFileSystemImplementationExpectations(
			CloudFileSystem fileSystem, boolean createPathFileSystemExpectations, CloudPath... paths) {
		int counter = cloudFileSystemImplementationExpectationsCounter.getAndIncrement();
		CloudFileSystemImplementation cloudFileSystemImplementation =
				context.mock(CloudFileSystemImplementation.class, "cloudFileSystemImplementation-" + counter);
		CloudHostConfiguration cloudHostConfiguration = context.mock(CloudHostConfiguration.class, "cloudHostConfiguration-" + counter);

		for (CloudPath path : paths) {
			context.checking(new Expectations() {{
				if (createPathFileSystemExpectations) {
					allowing(path).getFileSystem();
					will(returnValue(fileSystem));
				}
				
				allowing(fileSystem).getCloudHostConfiguration();
				will(returnValue(cloudHostConfiguration));
				
				allowing(cloudHostConfiguration).getCloudFileSystemImplementation();
				will(returnValue(cloudFileSystemImplementation));
			}});
		}

		return cloudFileSystemImplementation;
	}

	@Test
	public void testSplitPathsByImplementationWillCreateAMapCollectedByTheImplementationForMultiplePaths() {
		// The first collection should contain path1, path2, path3
		CloudPath path1 = context.mock(CloudPath.class, "path1");
		CloudPath path2 = context.mock(CloudPath.class, "path2");
		CloudPath path3 = context.mock(CloudPath.class, "path3");
		CloudFileSystemImplementation cloudFileSystemImplementation1 =
				createGetCloudFileSystemImplementationExpectations(path1, path2, path3);

		// The second collection should contain path4
		CloudPath path4 = context.mock(CloudPath.class, "path4");
		CloudFileSystemImplementation cloudFileSystemImplementation2 =
				createGetCloudFileSystemImplementationExpectations(path4);

		// The third collection should contain path5, path6
		CloudPath path5 = context.mock(CloudPath.class, "path5");
		CloudPath path6 = context.mock(CloudPath.class, "path6");
		CloudFileSystemImplementation cloudFileSystemImplementation3 =
				createGetCloudFileSystemImplementationExpectations(path5, path6);

		Map<CloudFileSystemImplementation, Collection<CloudPath>> pathsByImplementationMap =
				impl.splitPathsByImplementation(Sets.newHashSet(path1, path2, path3, path4, path5, path6));
		Assert.assertEquals(3, pathsByImplementationMap.size());
		
		// First set
		Collection<CloudPath> pathSet1 = pathsByImplementationMap.get(cloudFileSystemImplementation1);
		Assert.assertEquals(3, pathSet1.size());
		Assert.assertTrue(pathSet1.contains(path1));
		Assert.assertTrue(pathSet1.contains(path2));
		Assert.assertTrue(pathSet1.contains(path3));

		// Second set
		Collection<CloudPath> pathSet2 = pathsByImplementationMap.get(cloudFileSystemImplementation2);
		Assert.assertEquals(1, pathSet2.size());
		Assert.assertTrue(pathSet2.contains(path4));

		// Third set
		Collection<CloudPath> pathSet3 = pathsByImplementationMap.get(cloudFileSystemImplementation3);
		Assert.assertEquals(2, pathSet3.size());
		Assert.assertTrue(pathSet3.contains(path5));
		Assert.assertTrue(pathSet3.contains(path6));
	}
	
	private CloudPath createCloudPathWithBlobStoreContextExpectations(CloudFileSystem fileSystem,
			BlobStoreContext blobStoreContext, String name) {
		CloudPath path = context.mock(CloudPath.class, name);
		
		context.checking(new Expectations() {{
			allowing(path).getFileSystem();
			will(returnValue(fileSystem));
			
			allowing(fileSystem).getBlobStoreContext();
			will(returnValue(blobStoreContext));
		}});

		return path;
	}

	@Test
	public void testDeleteASetofPathsWillDeleteByCloudFileSystemImplementation() throws IOException {
		BlobStoreContext blobStoreContext1 = context.mock(BlobStoreContext.class, "blobStoreContext-1");
		CloudFileSystem fileSystem1 = context.mock(CloudFileSystem.class, "fileSystem-1");
		
		// The first collection should contain path1, path2, path3
		CloudPath path1 = createCloudPathWithBlobStoreContextExpectations(fileSystem1, blobStoreContext1, "path1");
		CloudPath path2 = createCloudPathWithBlobStoreContextExpectations(fileSystem1, blobStoreContext1, "path2");
		CloudPath path3 = createCloudPathWithBlobStoreContextExpectations(fileSystem1, blobStoreContext1, "path3");
		CloudFileSystemImplementation cloudFileSystemImplementation1 =
				createGetCloudFileSystemImplementationExpectations(fileSystem1, false, path1, path2, path3);
		Set<CloudPath> pathSet1 = Sets.newHashSet(path1, path2, path3);

		// The second collection should contain path4
		BlobStoreContext blobStoreContext2 = context.mock(BlobStoreContext.class, "blobStoreContext-2");
		CloudFileSystem fileSystem2 = context.mock(CloudFileSystem.class, "fileSystem-2");
		CloudPath path4 = createCloudPathWithBlobStoreContextExpectations(fileSystem2, blobStoreContext2, "path4");
		CloudFileSystemImplementation cloudFileSystemImplementation2 =
				createGetCloudFileSystemImplementationExpectations(fileSystem2, false, path4);
		Set<CloudPath> pathSet2 = new HashSet<CloudPath>();
		pathSet2.add(path4);

		EnumSet<DeleteOption> opts = EnumSet.of(DeleteOption.FAIL_SILENTLY);

		context.checking(new Expectations() {{
			exactly(1).of(cloudFileSystemImplementation1).delete(blobStoreContext1, pathSet1, opts);

			exactly(1).of(cloudFileSystemImplementation2).delete(blobStoreContext2, pathSet2, opts);
		}});
		
		Set<CloudPath> paths = Sets.newHashSet(path1, path2, path3, path4);
		impl.delete(paths, opts);
	}
	
	CloudPath createMockCloudPathWithCanOptimiseOperationsBetween(CloudPath targetPath, boolean canOptimise) {
		CloudPath path = context.mock(CloudPath.class, "cloudPath-" + cloudPathExpectationsCounter.getAndIncrement());

		context.checking(new Expectations() {{
			allowing(path).canOptimiseOperationsBetween(targetPath);
			will(returnValue(canOptimise));
		}});

		return path;
	}

	@Test
	public void testCopyMultiplePathsWillPerformTwoCopiesForADegradedAndAnOptimisedCopy() throws IOException {
		CloudPath targetPath = context.mock(CloudPath.class, "targetPath");
		CloudPath path1 = createMockCloudPathWithCanOptimiseOperationsBetween(targetPath, true);
		CloudPath path2 = createMockCloudPathWithCanOptimiseOperationsBetween(targetPath, false);
		CloudPath path3 = createMockCloudPathWithCanOptimiseOperationsBetween(targetPath, true);

		Set<CopyOption> originalCopyOpts = Sets.newHashSet(CloudCopyOption.FAIL_SILENTLY, CloudCopyOption.RECURSIVE);
		Set<CopyOption> degradedCopyOpts = Sets.newHashSet(originalCopyOpts);
		degradedCopyOpts.add(CloudCopyOption.DONT_RETURN_COPY_METHOD);
		Set<CopyOption> optimisedCopyOpts = Sets.newHashSet(degradedCopyOpts);

		BlobStoreContext blobStoreContext = context.mock(BlobStoreContext.class, "blobStoreContext-1");
		CloudFileSystem fileSystem = context.mock(CloudFileSystem.class, "fileSystem-1");
		CloudFileSystemImplementation cloudFileSystemImplementation =
				createGetCloudFileSystemImplementationExpectations(fileSystem, true, path1, path2, path3);
		
		context.checking(new Expectations() {{
			allowing(fileSystem).getBlobStoreContext();
			will(returnValue(blobStoreContext));

			exactly(1).of(cloudFileSystemImplementation).copy(blobStoreContext,
					Sets.newHashSet(path1, path2, path3), targetPath, optimisedCopyOpts);
		}});

		impl.copy(Sets.newHashSet(path1, path2, path3), targetPath, originalCopyOpts);
	}


}
