package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.security.Principal;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.hash.HashCode;
import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

/**
 * <pre>
 * list [PATH]
 * </pre>
 * @param arguments
 */
public class ListCommand implements CliCommand {

	public String getCommandName() {
		return "list";
	}
	
	public void printHelp(PrintWriter out) {
		out.println("Provides a listing for a container/directory.");
		out.println("\t- List files on the local filesystem:");
		out.println("\t\tlist file:///dir1/dir2");
		out.println("\t- List files on a cloud filesystem mounted as 's3-host':");
		out.println("\t\tlist cloud://s3-host/container/dir");
	}

	@Override
	public boolean execute(String[] commandArguments) {
    	if (commandArguments.length < 2) {
    		System.err.println("Not enough arguments");
    		return false;
    	}

    	URI uri;
    	try {
			uri = new URI(commandArguments[1]);
		} catch (URISyntaxException e) {
			System.err.println("Could not parse filesystem URI: " + e.getMessage());
			return false;
		}

    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
    	FileSystem fileSystem = Cli.getFileSystem(uri);
    	if (fileSystem == null) {
    		System.err.println("No file system alias called '" + uri.getHost());
    	}

    	int pathsCounter = 0;
    	if (uri.getPath().equals("/")) {
    		Iterable<Path> rootDirectories = fileSystem.getRootDirectories();
    		for (Path rootDir : rootDirectories) {
    			printCloudPathAttributes(fileSystem, rootDir);
    			pathsCounter++;
    		}
    	} else {
    		FileSystemProvider provider = fileSystem.provider();
			Path path = provider.getPath(uri);
			
			if (Files.isRegularFile(path)) {
				// File listing
    			printCloudPathAttributes(fileSystem, path);
    			pathsCounter++;
			} else {
				// Directory listing
				DirectoryStream<Path> dirStream;
	    		try {
					dirStream = provider.newDirectoryStream(path, new Filter<Path>() {
						@Override
						public boolean accept(Path entry) throws IOException {
							return true;
						}
					});
				} catch (Exception e) {
					System.err.println("Could not list directories for path '" + uri.getPath() + "': " + e.getMessage());
					e.printStackTrace();
					return false;
				}

	    		// For each result in the dir 
	    		Iterator<Path> paths = dirStream.iterator();
	    		while (paths.hasNext()) {
	    			Path nextPath = paths.next();
	    			printCloudPathAttributes(fileSystem, nextPath);
	    			pathsCounter++;
	    		}
			}
    	}
    	
    	System.out.println(pathsCounter + " total results");
    	return true;
	}

	protected void printCloudPathAttributes(FileSystem fileSystem, Path path) {
		CloudFileAttributesView fileAttributeView =
				fileSystem.provider().getFileAttributeView(path, CloudFileAttributesView.class);

		if (fileAttributeView == null) {
			printBasicPathAttributes(fileSystem, path);
			return;
		}

		CloudAclFileAttributes readAttributes;
		try {
			readAttributes = fileAttributeView.readAttributes();
		} catch (IOException e) {
			System.err.println("Could not read file attributes for '" + path.toString() + "'");
			return;
		}

		StringBuilder listing = new StringBuilder();
		listing.append(path.toString());
		if (readAttributes.isContainer() || readAttributes.isDirectory()) {
			listing.append(fileSystem.getSeparator());
		}

		listing.append(" ");
		listing.append(StringUtils.defaultIfBlank(readAttributes.getContentDisposition(), "-"));
		listing.append(" ");
		listing.append(StringUtils.defaultIfBlank(readAttributes.getContentEncoding(), "-"));
		listing.append(" ");
		listing.append(StringUtils.defaultIfBlank(readAttributes.getContentLanguage(), "-"));
		listing.append(" ");
		HashCode contentMD5 = readAttributes.getContentMD5();
		if (contentMD5 == null) {
			listing.append("-");
		} else {
			listing.append(contentMD5.toString());
		}
		listing.append(" ");
		listing.append(StringUtils.defaultIfBlank(readAttributes.getContentType(), "-"));
		listing.append(" ");

		listing.append(" owners=[");
		boolean flag = false;
		for (Principal principal : readAttributes.getOwners()) {
			if (flag) {
				listing.append(", ");
			} else {
				flag = true;
			}
			listing.append("'");
			listing.append(principal.getName());
			listing.append("'");
		}
		listing.append("] acls=[");

		flag = false;
		Iterator<CloudAclEntry<?>> aclSetIterator = readAttributes.getAclSet().iterator();
		while (aclSetIterator.hasNext()) {
			CloudAclEntry<?> nextEntry = aclSetIterator.next();
			if (flag) {
				listing.append(", ");
			} else {
				flag = true;
			}
			listing.append("{'");
			listing.append(nextEntry.getPrincipal().getName());
			listing.append("': ");
			listing.append(nextEntry.getPermissions().toString());
			listing.append("}");
		}
		listing.append("]");

		System.out.println(listing.toString());
	}

	private void printBasicPathAttributes(FileSystem fileSystem, Path path) {
		BasicFileAttributeView fileAttributeView =
				fileSystem.provider().getFileAttributeView(path, BasicFileAttributeView.class);
		if (fileAttributeView == null) {
			System.err.println("Cannot get file attributes for path '" + path.toString() +
					"' on filesystem type " + fileSystem.provider().getScheme());
			return;
		}
		
		BasicFileAttributes attributes;
		try {
			attributes = fileAttributeView.readAttributes();
		} catch (IOException e) {
			System.err.println("Could not read file attributes for '" + path.toString() + "'");
			return;
		}
		
		DateTimeFormatter dtFormat = ISODateTimeFormat.dateHourMinuteSecondMillis();
		StringBuilder listing = new StringBuilder();
		listing.append(path.toString());
		if (attributes.isDirectory()) {
			listing.append(fileSystem.getSeparator());
			listing.append(" - ");
		} else {
			listing.append(" ")
				.append(attributes.size())
				.append(" ");
		}

		listing.append(" ");
		if (attributes.creationTime() != null) {
			listing.append(dtFormat.print(attributes.creationTime().toMillis()));
		} else {
			listing.append("-");
		}

		listing.append(" ");
		if (attributes.lastModifiedTime() != null) {
			listing.append(dtFormat.print(attributes.lastModifiedTime().toMillis()));
		} else {
			listing.append("-");
		}
		
		System.out.println(listing.toString());
	}

}
