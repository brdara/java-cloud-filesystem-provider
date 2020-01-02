# Code Outline

## Configuration

The `com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration` class is used to provide configuration information for the environment. This is a slight abstraction over the JClouds API and provides additional helpers to make the code transferrable and reusable across all of the supported providers. The provided configurations are for:
- AWS: `com.uk.xarixa.cloud.filesystem.core.host.configuration.AwsCloudHostConfiguration`
- Azure: `com.uk.xarixa.cloud.filesystem.core.host.configuration.AzureCloudHostConfiguration`
- OpenStack: `com.uk.xarixa.cloud.filesystem.core.host.configuration.OpenStackCloudHostConfiguration`

Each of the providers has a method to create the BLOB store, and to do any required custom implementation there, and to retrieve the `com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation` which can be used to customize cloud actions. The methods in `com.uk.xarixa.cloud.filesystem.core.nio.DefaultCloudFileSystemImplementation` should usually provide most of the required functionality.

Each of the providers is annotated with `com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType` which names the configuration. Providing your own type is as simple as creating a new configuration and annotation. The types are searched for in the `com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfigurationBuilder` using a classpath scanner to find all classes implementing the `com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration` containing the annotation.

Creating a configuration is as simple as searching for the configuration type and passing in the desired parameters. The builder uses introspection to find setters for the parameters on the specific configuration class. For example the `com.uk.xarixa.cloud.filesystem.core.host.configuration.AwsCloudHostConfiguration` has setters for `accessKey` and `secretKey` and so these can be passed into the configuration. To create an AWS configuration through the builder would follow this example:

```
CloudHostConfiguration configuration = new CloudHostConfigurationBuilder()
			.setType("AWS")
			.setName("live-test")
			.setAttribute("accessKey", "XXXXXXXXXXXXXXXXX")
			.setAttribute("secretKey", "XXXXXXXXXXXXXXXXXXXXX")
			.build();
```

## BLOB Store

Once a configuration has been instantiated then the JClouds org.jclouds.blobstore.BlobStoreContext can be retrieved from the configuration:

```	
BlobStoreContext context = configuration.createBlobStoreContext();
```

## File system operations

File system operations are carried out through an implementation of `java.nio.file.FileSystem` in the class `com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystem`. This can be manually instantiated once the configuration and JClouds BLOB store context have been created thus:

```
JCloudsCloudHostProvider provider = new JCloudsCloudHostProvider();
CloudFileSystem cloudFileSystem = new CloudFileSystem(provider, configuration, context)
```

Cloud file system operations can then be carried out using the provider to perform actions on the file system.

## Java NIO File system SPI

The file system is wired into the Java NIO file system SPI through the `java.nio.file.spi.FileSystemProvider` which is implemented through the `com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProviderDelegate`. Therefore once the JAR is included in any Java environment, the cloud file system will be available, for example if we wanted to create a filesystem with the alias __cloud-filesystem__:

```
// Create an environment map which will pass attributes to the com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration
Map<String,Object> env = new HashMap<>();
env.put(JCloudsCloudHostProvider.CLOUD_TYPE_ENV, "AWS");
env.put("accessKey", "XXXXXXXXXXXXXXXXX");
env.put("secretKey", "XXXXXXXXXXXXXXXXXXXXX");

// Create a URI to the filesystem, which will be aliased as "cloud-filesystem"
URI uri = new URI(CloudFileSystemProviderDelegate.CLOUD_SCHEME, "cloud-filesystem", "/", null);

// Access the file system through the SPI
FileSystem fs = FileSystemProviderHelper.newFileSystem(uri, env, CloudFileSystemProviderDelegate.class.getClassLoader());
```


## Integration Tests

The integration tests can be run by setting up a provider and settings in the Maven settings file, Eclipse runtime options, or the JVM - see the `com.uk.xarixa.cloud.filesystem.core.CloudFileSystemLiveTestHelper` for details on how to run this for AWS provider tests.

The `com.uk.xarixa.cloud.filesystem.core.nio.DefaultCloudFileSystemImplementationIntegrationTest` shows the different usages for setting metadata and ACL's on the files.