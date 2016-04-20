package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.AwsCloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.AzureCloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfigurationBuilder;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.OpenStackCloudHostConfiguration;

@RunWith(BlockJUnit4ClassRunner.class)
public class CloudHostConfigurationBuilderTest {

	@Test
	public void testGetCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotationWillReturnTheAnnotationValueForAClass() {
		Assert.assertEquals("AWS",
				CloudHostConfigurationBuilder.getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(AwsCloudHostConfiguration.class));
		Assert.assertEquals("Azure",
				CloudHostConfigurationBuilder.getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(AzureCloudHostConfiguration.class));
		Assert.assertEquals("OpenStack",
				CloudHostConfigurationBuilder.getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(OpenStackCloudHostConfiguration.class));
	}
	
	@Test
	public void testNewCloudHostSettingsTypesAreDiscoveredOnTheClasspath() {
		Assert.assertEquals(MockCloudHostConfiguration.class, CloudHostConfigurationBuilder.getCloudHostConfigurationClass("mock-test"));
		Assert.assertEquals("mock-test", CloudHostConfigurationBuilder.getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(MockCloudHostConfiguration.class));
	}
	
	@Test
	public void testGetCloudHostConfigurationClassDiscoversTheCloudSettingsClass() {
		Assert.assertEquals(AwsCloudHostConfiguration.class, CloudHostConfigurationBuilder.getCloudHostConfigurationClass("AWS"));
		Assert.assertEquals(AzureCloudHostConfiguration.class, CloudHostConfigurationBuilder.getCloudHostConfigurationClass("Azure"));
		Assert.assertEquals(OpenStackCloudHostConfiguration.class, CloudHostConfigurationBuilder.getCloudHostConfigurationClass("OpenStack"));
	}
	
	@Test
	public void testGetCloudHostConfigurationClassReturnsNullIfTheSettingsClassCannotBeFound() {
		Assert.assertNull(CloudHostConfigurationBuilder.getCloudHostConfigurationClass("NonExistant"));
	}
	
	@Test
	public void testGetAllCloudHostSettingTypesReturnsAllTypes() {
		List<String> allCloudHostSettingTypes = CloudHostConfigurationBuilder.getAllCloudHostSettingTypes();
		Assert.assertFalse(allCloudHostSettingTypes.isEmpty());
		Assert.assertTrue(allCloudHostSettingTypes.contains("AWS"));
		Assert.assertTrue(allCloudHostSettingTypes.contains("Azure"));
		Assert.assertTrue(allCloudHostSettingTypes.contains("OpenStack"));
	}

	@Test
	public void testCanCreateAwsCloudHostSettingsType() {
		AwsCloudHostConfiguration settings = (AwsCloudHostConfiguration)new CloudHostConfigurationBuilder()
			.setType("AWS")
			.setName("aws-v1")
			.setAttribute("accessKey", "test-access-key")
			.setAttribute("secretKey", "test-secret-key")
			.build();
		
		Assert.assertNotNull(settings);
		Assert.assertEquals("aws-v1", settings.getName());
		Assert.assertEquals("test-access-key", settings.getAccessKey());
		Assert.assertEquals("test-secret-key", settings.getSecretKey());
	}

	@Test
	public void testCanCreateOpenStackCloudHostSettingsType() {
		OpenStackCloudHostConfiguration settings = (OpenStackCloudHostConfiguration)new CloudHostConfigurationBuilder()
			.setType("OpenStack")
			.setName("openstack-v1")
			.setAttribute("endpoint", "test-endpoint")
			.setAttribute("identity", "test-identity")
			.setAttribute("credential", "test-credential")
			.build();
		
		Assert.assertNotNull(settings);
		Assert.assertEquals("openstack-v1", settings.getName());
		Assert.assertEquals("test-endpoint", settings.getEndpoint());
		Assert.assertEquals("test-identity", settings.getIdentity());
		Assert.assertEquals("test-credential", settings.getCredential());
	}

}
