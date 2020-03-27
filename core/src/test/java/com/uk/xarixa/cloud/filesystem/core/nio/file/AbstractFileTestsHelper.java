package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.imposters.ByteBuddyClassImposteriser;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;

import com.google.common.hash.HashCode;
import com.scalified.tree.TreeNode;
import com.scalified.tree.multinode.ArrayMultiTreeNode;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

public abstract class AbstractFileTestsHelper {
	private final AtomicInteger pathCounter = new AtomicInteger(0);

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ByteBuddyClassImposteriser.INSTANCE);
	}};

	FileSystemProvider fsProvider;
	CloudFileSystem fs;

	@Before
	public final void setUp() {
		preSetUp();

		fs = context.mock(CloudFileSystem.class);
		fsProvider = context.mock(FileSystemProvider.class);
		
		context.checking(new Expectations() {{
			allowing(fs).provider(); will(returnValue(fsProvider));
		}});
		
		postSetUp();
	}
	
	abstract void preSetUp();

	abstract void postSetUp();

	TreeNode<TrackedFileEntry> createTreeNodeForDirectoryPath(String path) throws IOException {
		CloudPath cloudPath = createCloudPath(path, true);
		TrackedFileEntry entry = new TrackedFileEntry(cloudPath, true);
		return new ArrayMultiTreeNode<>(entry);
	}
	
	TreeNode<TrackedFileEntry> createTreeNodeForFilePath(String path, String checksum) throws IOException {
		CloudPath cloudPath = createCloudPath(path, true);
		TrackedFileEntry entry = new TrackedFileEntry(cloudPath, false, checksum);
		return new ArrayMultiTreeNode<>(entry);
	}
	
	CloudPath createCloudPath(String path, boolean isDirectory) throws IOException {
		return createCloudPath(path, isDirectory, StringUtils.EMPTY);
	}

	CloudPath createCloudPath(String path, boolean isDirectory, String checkSum) throws IOException {
		CloudPath cloudPath = new CloudPath(fs, true, path);
		BasicFileAttributes fileAttrMock = context.mock(BasicFileAttributes.class, pathCounter.incrementAndGet() + "-BasicFileAttributes-" + path);
		CloudFileAttributesView fileAttrView = context.mock(CloudFileAttributesView.class, pathCounter.get() + "-CloudFileAttributesView-" + path);
		CloudAclFileAttributes aclFileAttr = context.mock(CloudAclFileAttributes.class, pathCounter.get() + "-CloudAclFileAttributes-" + path);
		HashCode hashCodeMock = StringUtils.isEmpty(checkSum) ? null : HashCode.fromString(checkSum);

		context.checking(new Expectations() {{
			atMost(4).of(fsProvider).readAttributes(cloudPath, BasicFileAttributes.class);
			will(returnValue(fileAttrMock));
			
			atMost(4).of(fileAttrMock).isDirectory();
			will(returnValue(isDirectory));

			if (!isDirectory) {
				final Matcher<LinkOption[]> linkOptArrayMatcher = anything();
				atMost(1).of(fsProvider).getFileAttributeView(with(equal(cloudPath)), with(equal(CloudFileAttributesView.class)), with(linkOptArrayMatcher));
				will(returnValue(fileAttrView));

				atMost(1).of(fileAttrView).readAttributes(); will(returnValue(aclFileAttr));

				atMost(1).of(aclFileAttr).getContentMD5(); will(returnValue(hashCodeMock));
			}
		}});

		return cloudPath;
	}
	
	String createMd5Digest(String digestContent) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Unable to create MD5 message digest", e);
		}

		md.update(digestContent.getBytes());
		return Hex.encodeHexString(md.digest());
	}

	DirectoryStream<Path> createDirectoryListing(Path directory, Path... subPaths) throws IOException {
		@SuppressWarnings("unchecked")
		DirectoryStream<Path> listing = context.mock(DirectoryStream.class, pathCounter.incrementAndGet() + "-DirectoryStream-" + directory);
		Iterator<Path> listingIterator = Arrays.asList(subPaths).iterator();

		context.checking(new Expectations() {{
			exactly(1).of(fsProvider).newDirectoryStream(directory, PathFilters.ACCEPT_ALL_FILTER); will(returnValue(listing));
			
			allowing(listing).iterator(); will(returnValue(listingIterator));
			
			allowing(listing).close();
		}});
		
		return listing;
	}

}
