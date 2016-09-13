package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.security.Principal;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

/**
 * <pre>
 * list [PATH]
 * </pre>
 * @param arguments
 */
public class ListCommand extends AbstractCliCommand {
	private static final Logger LOG = LoggerFactory.getLogger(ListCommand.class);
	private static final String RECURSIVE_OPTION = "recursive";
	private static final List<String> options = Lists.newArrayList(RECURSIVE_OPTION);
	public static final Filter<Path> acceptAllFilter =
											new Filter<Path>() {
												@Override
												public boolean accept(Path entry) throws IOException {
													return true;
												}
											};

	public String getCommandName() {
		return "list";
	}
	
	@Override
	public void printSummaryHelp(PrintWriter out) {
		out.println("Lists files or directories");
	}

	public void printFullHelp(PrintWriter out) {
		out.println("Provides a listing for a container/directory.");
		out.println("\t- List files on the local filesystem:");
		out.println("\t\tlist file:///dir1/dir2");
		out.println("\t- List files on a cloud filesystem mounted as 's3-host':");
		out.println("\t\tlist cloud://s3-host/container/dir");
		out.println("\t- List files recursively in all directories and sub-directories on a cloud"
				+ "filesystem mounted as 's3-host':");
		out.println("\t\tlist --recursive cloud://s3-host/container/dir");
	}
	
	@Override
	public List<String> getCommandOptions() {
		return options;
	}
	
	@Override
	public int getMinimumNumberOfParameters() {
		return 1;
	}

	@Override
	public int getMaximumNumberOfParameters() {
		return -1;
	}

	@Override
	public boolean executeCommand(ParsedCommand parsedCommand) {
		boolean recursive = parsedCommand.getCommandOptions().contains(RECURSIVE_OPTION);
    	int pathsCounter = 0;
		List<String> commandParameters = parsedCommand.getCommandParameters();
		
		for (String commandArg : commandParameters) {
	    	URI uri;
	    	try {
				uri = new URI(commandArg);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI: " + e.getMessage());
				return false;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = Cli.getFileSystem(uri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + uri.getHost() + "'");
	    	}
	
    		FileSystemProvider provider = fileSystem.provider();
	    	if (uri.getPath().equals(fileSystem.getSeparator())) {
				pathsCounter += listDirectoryContents(provider, fileSystem, null, recursive);
	    	} else {
				Path path = provider.getPath(uri);
				
				if (Files.isRegularFile(path)) {
					// File listing
	    			printCloudPathAttributes(fileSystem, path);
	    			pathsCounter++;
				} else {
					pathsCounter += listDirectoryContents(provider, fileSystem, path, recursive);
				}
	    	}

		}

    	System.out.println(pathsCounter + " total results");
		return true;
	}
	
	protected int listDirectoryContents(FileSystemProvider provider, FileSystem fileSystem, Path path, boolean recursive) {
		AtomicInteger pathsCounter = new AtomicInteger(0);

		FileSystemProviderHelper.iterateOverDirectoryContents(fileSystem, Optional.ofNullable(path),
				acceptAllFilter, recursive,
					subPath -> {
						pathsCounter.addAndGet(printCloudPathAttributes(fileSystem, subPath.getResultPath()));
						return true;
					});

		return pathsCounter.get();
	}

	protected int printCloudPathAttributes(FileSystem fileSystem, Path path) {
		CloudFileAttributesView fileAttributeView =
				fileSystem.provider().getFileAttributeView(path, CloudFileAttributesView.class);

		if (fileAttributeView == null) {
			return printBasicPathAttributes(fileSystem, path);
		}

		CloudAclFileAttributes readAttributes;
		try {
			readAttributes = fileAttributeView.readAttributes();
		} catch (IOException e) {
			System.err.println("Could not read file attributes for '" + path.toString() + "'");
			return 0;
		}

		StringBuilder listing = new StringBuilder();
		listing.append(path.toAbsolutePath().toString());
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
		return 1;
	}

	private int printBasicPathAttributes(FileSystem fileSystem, Path path) {
		BasicFileAttributeView fileAttributeView =
				fileSystem.provider().getFileAttributeView(path, BasicFileAttributeView.class);
		if (fileAttributeView == null) {
			System.err.println("Cannot get file attributes for path '" + path.toString() +
					"' on filesystem type " + fileSystem.provider().getScheme());
			return 0;
		}
		
		BasicFileAttributes attributes;
		try {
			attributes = fileAttributeView.readAttributes();
		} catch (IOException e) {
			System.err.println("Could not read file attributes for '" + path.toString() + "'");
			return 0;
		}
		
		DateTimeFormatter dtFormat = ISODateTimeFormat.dateHourMinuteSecondMillis();
		StringBuilder listing = new StringBuilder();
		listing.append(path.toAbsolutePath().toString());
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
		return 1;
	}

}
