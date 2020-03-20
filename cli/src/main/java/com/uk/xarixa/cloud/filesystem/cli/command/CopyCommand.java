package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.CommandOption;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPathException;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.options.CloudCopyOption;

public class CopyCommand extends AbstractCliCommand {
	private static final Logger LOG = LoggerFactory.getLogger(CopyCommand.class);
	private static final List<CommandOption> options = Lists.newArrayList(new CommandOption(RECURSIVE_OPTION));

	@Override
	public String getCommandName() {
		return "copy";
	}

	@Override
	public void printSummaryHelp(PrintWriter out) {
		out.println("Copies files or directories");
	}

	@Override
	public void printFullHelp(PrintWriter out) {
		out.println("Copies one or more files or directories from a filesystem to another filesystem");
		out.println("\t- Copy files on the local filesystem:");
		out.println("\t\tcopy file:///drive1/dir1/file.txt file:///drive2/dir2/file.txt");
		out.println("\t\tcopy file:///C:/dir1/file.txt file:///drive2/dir2/file.txt");
		out.println("\t- Copy files on the local filesystem recursively:");
		out.println("\t\tcopy --recursive file:///drive1/dir1/file.txt file:///drive2/dir2/file.txt");
		out.println("\t- Copy files and directories from recursively local filesystem to a cloud filesystem mounted as 's3-host':");
		out.println("\t\tcopy --recursive file:///drive1/dir1 cloud://s3-host/container/dir");
		out.println("\t- Copy files recursively in all directories and sub-directories on a cloud"
				+ "filesystem mounted as 's3-host' to a cloud filesystem mounted as 's3-host2':");
		out.println("\t\tcopy --recursive cloud://s3-host/container/dir cloud://s3-host2/container2/dir2");

		out.println("\t- Copy files recursively in all directories and sub-directories on a cloud"
				+ "filesystem mounted as 's3-host' with GLOB-style filtering to copy all files "
				+ "ending with '.java':");
		out.println("\t\tcopy --recursive --filter=glob:**/*.java cloud://s3-host/container/dir");
		out.println("\t- Copy files recursively in all directories and sub-directories on a cloud"
				+ "filesystem mounted as 's3-host' with REGEX-style filtering to copy all files "
				+ "ending with '.java':");
		out.println("\t\tcopy --recursive --filter=glob:.*\\.java cloud://s3-host/container/dir");
	}

	@Override
	public List<CommandOption> getCommandOptions() {
		return options;
	}

	@Override
	public int getMinimumNumberOfParameters() {
		return 2;
	}

	@Override
	public int getMaximumNumberOfParameters() {
		return -1;
	}

	@Override
	public boolean executeCommand(ParsedCommand parsedCommand) {
		boolean recursive = parsedCommand.getCommandOptionByName(RECURSIVE_OPTION) != null;

		// Get the destination from the final parameter
		List<String> commandParameters = parsedCommand.getCommandParameters();
    	String destination = commandParameters.get(commandParameters.size() - 1);

    	URI destinationUri;
    	try {
    		destinationUri = new URI(destination);
		} catch (URISyntaxException e) {
			System.err.println("Could not parse destination filesystem URI '" + destination + "': " + e.getMessage());
    		return false;
		}

    	for (int i=0; i<commandParameters.size() - 1; i++) {
	    	URI sourceUri;
	    	try {
				sourceUri = new URI(commandParameters.get(i));
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI '" + commandParameters.get(i) + "': " + e.getMessage());
	    		return false;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = Cli.getFileSystem(sourceUri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + sourceUri.getHost());
	    	}

	    	// Recreate filters as necessary
			Filter<Path> pathFilters =
					createPathFilters(parsedCommand.getCommandOptionsByName(FILTER_OPTION), fileSystem.getSeparator());

	    	FileSystemProvider sourceProvider = fileSystem.provider();
	    	Path sourcePath;
	    	try {
	    		sourcePath = sourceProvider.getPath(sourceUri);
	    	} catch (CloudPathException e) {
	    		System.err.println("The cloud path '" + destinationUri + "' is not valid, it must have a container path");
	    		return false;
	    	}

	    	FileSystemProvider destinationProvider = Cli.getFileSystem(destinationUri).provider();
	    	Path destinationPath;
	    	try {
	    		destinationPath = destinationProvider.getPath(destinationUri);
	    	} catch (CloudPathException e) {
	    		System.err.println("The cloud path '" + destinationUri + "' is not valid, it must have a container path");
	    		return false;
	    	}

	    	try {
	    		if (recursive && sourceProvider instanceof CloudFileSystemProvider && !isAcceptAllPathFilter(pathFilters)) {
		    		sourceProvider.copy(sourcePath, destinationPath,
			    			StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING, CloudCopyOption.RECURSIVE);
	    		} else {
	    			if (Files.isDirectory(sourcePath)) {
		    			LOG.debug("Copying directory from a source local filesystem");
		    			FileSystemProviderHelper.copyDirectoryBetweenProviders(sourcePath, destinationPath,
		    					pathFilters, recursive);
	    			} else {
		    			LOG.debug("Copying file from a source local filesystem");
		    			if (isAcceptAllPathFilter(pathFilters)) {
		    				LOG.warn("Ignoring specified path filters for direct file copy");
		    			}
		    			FileSystemProviderHelper.copyFileBetweenProviders(sourcePath, destinationPath);
	    			}
	    		}
	    	} catch (ProviderMismatchException e) {
	    		LOG.debug("Source provider with scheme {} cannot perform copy to {}, falling back to manual copy",
	    				sourceProvider.getScheme(), destinationProvider.getScheme());
		    	int filesCopied = 0;

		    	// Windows FS provider implemenation doesn't let you copy using it as the source
	    		// to another destintation provider implementation like our cloud one, da-da-dumb
	    		if (Files.isDirectory(sourcePath)) {
	    			LOG.debug("Copying directory from a source local filesystem");
	    			filesCopied = FileSystemProviderHelper.copyDirectoryBetweenProviders(sourcePath,
	    					destinationPath, pathFilters, recursive);
	    		} else {
	    			LOG.debug("Copying file from a source local filesystem");
	    			if (isAcceptAllPathFilter(pathFilters)) {
	    				LOG.warn("Ignoring specified path filters for direct file copy");
	    			}
	    			filesCopied = FileSystemProviderHelper.copyFileBetweenProviders(sourcePath, destinationPath);
	    		}
	    		
	    		System.out.println(filesCopied + " files copied");
	    		
	    	} catch (Exception e) {
				System.err.println("Could not create directory '" + sourceUri.toString() + "': " + e.getMessage());
				e.printStackTrace();
	    		return false;
			}
    	}

    	return true;
	}

}
