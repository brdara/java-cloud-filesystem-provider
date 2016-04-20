package com.uk.xarixa.cloud.filesystem.core;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.jclouds.blobstore.BlobStoreContext;
import org.junit.Assert;

import com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfigurationBuilder;

/**
 * Run from the command line or using Maven settings.xml with properties:
 * <ul>
 * <li>livetest.type - Corresponds to one of the annotated {@link CloudHostConfigurationType}
 * <li>livetest.PROPERTY - Each property corresponds to the bean name of a property on the {@link CloudHostConfigurationType} bean
 * </ul>
 * For example, for AWS:
 * <pre>
 * -Dlivetest.type=AWS -Dlivetest.accessKey=[AWS_ACCESS_KEY] -Dlivetest.secretKey=[AWS_SECRET_KEY]
 * </pre>
 */
public abstract class CloudFileSystemLiveTestHelper {
	private static final String LIVETEST_PREFIX = "livetest.";
	private static final String LIVETEST_TYPE = LIVETEST_PREFIX + "type";
	
	private CloudFileSystemLiveTestHelper() {}

	public final static BlobStoreContext createBlobStoreContext() {
		CloudHostConfiguration cloudHostSettings = getCloudHostSettings();
		return createBlobStoreContext(cloudHostSettings);
	}

	public final static BlobStoreContext createBlobStoreContext(CloudHostConfiguration cloudHostSettings) {
		return cloudHostSettings.createBlobStoreContext();
	}

	public final static CloudHostConfiguration getCloudHostSettings() {
		Class<? extends CloudHostConfiguration> cloudHostSettingsClass =
				CloudHostConfigurationBuilder.getCloudHostConfigurationClass(System.getProperty(LIVETEST_TYPE));
		Assert.assertNotNull("Could not find cloud host settings for type '" + System.getProperty(LIVETEST_TYPE) + "' (" + LIVETEST_TYPE + " system property)",
				cloudHostSettingsClass);
		CloudHostConfigurationBuilder builder = new CloudHostConfigurationBuilder()
			.setType(cloudHostSettingsClass)
			.setName("live-test");
		
		Properties properties = System.getProperties();
		properties.entrySet()
	        .stream()
	        .filter(p -> ((String)p.getKey()).startsWith(LIVETEST_PREFIX) && !((String)p.getKey()).equals(LIVETEST_TYPE))
	        .forEach(p -> builder.setAttribute(StringUtils.removeStart((String)p.getKey(), LIVETEST_PREFIX), (String)p.getValue()));
		return builder.build();
	}

}
