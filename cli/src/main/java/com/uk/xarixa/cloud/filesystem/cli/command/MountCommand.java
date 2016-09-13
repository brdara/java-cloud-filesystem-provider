package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.uk.xarixa.cloud.filesystem.core.host.factory.JCloudsCloudHostProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProviderDelegate;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;

/**
 * Creates a CloudHostConfiguration:
 * <pre>
 * mount [ALIAS] [CLOUD_TYPE] attribute1=value1 ... attributeN=valueN
 * mount s3-user-1 AWS accessKey=XXXX secretKey=YYYY
 * </pre>
 */
public class MountCommand implements CliCommand {

	@Override
	public String getCommandName() {
		return "mount";
	}

	@Override
	public void printSummaryHelp(PrintWriter out) {
		out.println("Mounts a cloud based filesystem");
	}

	@Override
	public void printFullHelp(PrintWriter out) {
		out.println("Mounts a cloud based filesystem using the specified parameters.");
		out.println("\t- Mount an AWS S3 cloud filesystem as 's3-host':");
		out.println("\t\tmount s3-host AWS accessKey=XXXXXXXXXXX secretKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
	}

	@Override
	public boolean execute(String[] commandArguments) {
    	if (commandArguments.length < 3) {
    		System.err.println("Not enough arguments");
			return false;
    	}
    	
    	String alias = commandArguments[1];
    	Map<String,Object> env = new HashMap<>();
    	env.put(JCloudsCloudHostProvider.CLOUD_TYPE_ENV, commandArguments[2]);

    	for (int i=3; i<commandArguments.length; i++) {
    		String[] avPair = StringUtils.split(commandArguments[i], "=");
    		if (avPair.length != 2) {
    			System.err.println("Command line '" + StringUtils.join(commandArguments,  ' ') +
    					"' contains illegal attribute-value pair at '" + commandArguments[i] + "'");
    			return false;
    		}

    		env.put(avPair[0], avPair[1]);
    	}

    	URI uri;
    	try {
			uri = new URI(CloudFileSystemProviderDelegate.CLOUD_SCHEME, alias, "/", null);
		} catch (URISyntaxException e) {
			System.err.println("Could not create URI for alias '" + alias + "': " + e.getMessage());
			return false;
		}

    	FileSystem fs;
    	try {
			fs =
					FileSystemProviderHelper.newFileSystem(uri, env, CloudFileSystemProviderDelegate.class.getClassLoader());
//					FileSystems.newFileSystem(uri, env, CloudFileSystemProviderDelegate.class.getClassLoader());
		} catch (Exception e) {
			System.err.println("Could not create file system for alias '" + alias + "' with env " +
					env.toString() + " : " + e.getMessage());
			return false;
		}
    	
    	System.out.println("Cloud filesystem mounted at " + uri.toString() + " {scheme = " +
    			fs.provider().getScheme() + ", provider=" + fs.provider().getClass().getName() + "}");
    	return true;
	}

}
