package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryFlag;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.UserPrincipal;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntryBuilder;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudAclEntryBuilderTest {

	@Test
	public void testCloneProducesAnEquivalentAclEntry() {
		TestUserImpl testUser = new TestUserImpl("user1");
		CloudAclEntryBuilder<UserPrincipal> builder = new CloudAclEntryBuilder<UserPrincipal>(UserPrincipal.class)
			.addFlag(AclEntryFlag.DIRECTORY_INHERIT)
			.setPrincipal(testUser)
			.setType(AclEntryType.ALLOW);
		CloudAclEntry<UserPrincipal> unclonedEntry = builder.build();
		CloudAclEntry<UserPrincipal> clonedEntry = builder.clone().build();
		Assert.assertEquals(unclonedEntry, clonedEntry);
	}

}
