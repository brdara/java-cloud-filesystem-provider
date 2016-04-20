package com.uk.xarixa.cloud.filesystem.core.file.attribute;

import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;

import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.options.PutOptions.ImmutablePutOptions;
import org.jclouds.s3.domain.AccessControlList;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.net.MediaType;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.CloudPermissionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.ContentTypeFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.FileAttributeLookupMap;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.GetOptionFileAttribute;
import com.uk.xarixa.cloud.filesystem.core.file.attribute.PutOptionFileAttribute;

@RunWith(BlockJUnit4ClassRunner.class)
public class FileAttributeLookupMapTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};

	@Test
	public void testClassKeyBuilderCreatesAKeyWithShiftedIntegerHashCodesIntoASingleLong() {
		String fileAttributeValue = Long.toBinaryString(GetOptionFileAttribute.class.hashCode());
		String getOptionsValue = Long.toBinaryString(GetOptions.class.hashCode());
		String result = Long.toBinaryString(FileAttributeLookupMap.classKeyBuilder(GetOptionFileAttribute.class, GetOptions.class));
		Assert.assertTrue(result.startsWith(fileAttributeValue));
		Assert.assertTrue(result.endsWith(getOptionsValue));
		//System.out.println(result);
	}
	
	@Test
	public void testGetFileAttributeOfTypeWillThrowAnExceptionForNullValues() {
		CloudPermissionFileAttribute<AccessControlList> permAcl = new CloudPermissionFileAttribute<AccessControlList>(null);
		
		try {
			new FileAttributeLookupMap(permAcl);
			Assert.fail("Did not expect to be able to have empty values for a file attribute");
		} catch (IllegalArgumentException e) {
			// OK
		}
	}

	@Test
	public void testGetFileAttributeOfType() {
		CloudPermissionFileAttribute<AccessControlList> permAcl =
				new CloudPermissionFileAttribute<AccessControlList>(new AccessControlList());
		CloudPermissionFileAttribute<PosixFilePermission> permPosix =
				new CloudPermissionFileAttribute<PosixFilePermission>(PosixFilePermission.GROUP_WRITE);
		ContentTypeFileAttribute contentTypeGif = new ContentTypeFileAttribute(MediaType.GIF);
		GetOptionFileAttribute getOption = new GetOptionFileAttribute(GetOptions.NONE);
		PutOptionFileAttribute putOption = new PutOptionFileAttribute(PutOptions.NONE);
		FileAttributeLookupMap lookupMap = new FileAttributeLookupMap(
				permAcl,
				permPosix,
				contentTypeGif,
				getOption,
				putOption);

		// Test getting them explicitly by type
		Assert.assertEquals(lookupMap.toString(),
				permAcl, lookupMap.getFileAttributeOfType(CloudPermissionFileAttribute.class, AccessControlList.class));
		Assert.assertEquals(lookupMap.toString(),
				permPosix, lookupMap.getFileAttributeOfType(CloudPermissionFileAttribute.class, PosixFilePermission.class));
		Assert.assertEquals(lookupMap.toString(),
				contentTypeGif, lookupMap.getFileAttributeOfType(ContentTypeFileAttribute.class, MediaType.class));
		Assert.assertEquals(lookupMap.toString(),
				getOption, lookupMap.getFileAttributeOfType(GetOptionFileAttribute.class, GetOptions.class));
		Assert.assertEquals(lookupMap.toString(),
				putOption, lookupMap.getFileAttributeOfType(PutOptionFileAttribute.class, ImmutablePutOptions.class));
		
		// Test getting all of the attributes of a type
		Collection<CloudPermissionFileAttribute> permAttributes =
				lookupMap.getFileAttributesOfType(CloudPermissionFileAttribute.class);
		Assert.assertEquals(2, permAttributes.size());
		Assert.assertTrue(permAttributes.contains(permAcl));
		Assert.assertTrue(permAttributes.contains(permPosix));
		
		Collection<ContentTypeFileAttribute> contentTypeAttributes =
				lookupMap.getFileAttributesOfType(ContentTypeFileAttribute.class);
		Assert.assertEquals(1, contentTypeAttributes.size());
		Assert.assertEquals(contentTypeGif, contentTypeAttributes.iterator().next());

		Collection<GetOptionFileAttribute> getOptionAttributes =
				lookupMap.getFileAttributesOfType(GetOptionFileAttribute.class);
		Assert.assertEquals(1, getOptionAttributes.size());
		Assert.assertEquals(getOption, getOptionAttributes.iterator().next());

		Collection<PutOptionFileAttribute> putOptionAttributes =
				lookupMap.getFileAttributesOfType(PutOptionFileAttribute.class);
		Assert.assertEquals(1, putOptionAttributes.size());
		Assert.assertEquals(putOption, putOptionAttributes.iterator().next());
	}
	
}
