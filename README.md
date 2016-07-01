# java-cloud-filesystem-provider
A Java FileSystem provider which uses JClouds for cloud connectivity. This currently supports the following Cloud environments:
- AWS S3
- Azure
- OpenStack

# Quickstart
Include the dependency in Maven:


# Documentation
Find documentation in each of the sub-modules. The "core" module is the place to start.

# Integration Tests
The integration tests can be run against

# Compatibility
- JDK 1.8 (there is some lambda code in there)
- Deployed and tested with JClouds 1.9.2

# Known Limitations
- ACL can only be set in AWS using the CloudFilePermission set to an instance of org.jclouds.s3.domain.AccessControlList
- CloudFileChannel.map method does not obey SYNC/DSYNC