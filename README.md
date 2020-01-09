# java-cloud-filesystem-provider
A Java FileSystem provider which uses JClouds for cloud connectivity. This currently has been tested and supports the following Cloud environments:
- AWS S3
- Azure
- OpenStack

This works using the [Java Filesystem provider SPI](https://docs.oracle.com/javase/8/docs/technotes/guides/io/fsp/filesystemprovider.html) and extends those capabilities with some custom commands.

# Prerequisites

- Maven 3.3.3
- JDK 1.8+

# Quickstart
Include the dependency in Maven:

Compile the release packages (the integration tests are skipped by default) using:

```
mvn clean package
```

To compile and skip all tests run with:

```
mvn clean package -DskipTests=true
```


# Documentation
Find documentation in each of the sub-modules. The [core module](core/README.md) is the place to start.

# Integration Tests
The integration tests can be run against

# Compatibility
- JDK 1.8 (there is some lambda code in there), JDK 11
- Deployed and tested with JClouds 2.2.0

# Known Limitations
- ACL can only be set in AWS using the CloudPermissionFileAttribute set to an instance of org.jclouds.s3.domain.AccessControlList
- CloudFileChannel.map method does not obey SYNC/DSYNC