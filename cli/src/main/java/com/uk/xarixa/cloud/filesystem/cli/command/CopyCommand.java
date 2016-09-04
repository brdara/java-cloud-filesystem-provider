package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.spi.FileSystemProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPathException;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;

public class CopyCommand implements CliCommand {
	private static final Logger LOG = LoggerFactory.getLogger(CopyCommand.class);

	@Override
	public String getCommandName() {
		return "copy";
	}

	@Override
	public void printHelp(PrintWriter out) {
		out.println("Copies one or more files or directories from a filesystem to another filesystem");
	}

	@Override
	public boolean execute(String[] commandArguments) {
    	if (commandArguments.length < 3) {
    		System.err.println("Not enough arguments");
    		return false;
    	}
    	
    	String destination = commandArguments[commandArguments.length - 1];
    	URI destinationUri;
    	try {
    		destinationUri = new URI(destination);
		} catch (URISyntaxException e) {
			System.err.println("Could not parse destination filesystem URI '" + destination + "': " + e.getMessage());
    		return false;
		}

    	for (int i=1; i<commandArguments.length - 1; i++) {
	    	URI sourceUri;
	    	try {
				sourceUri = new URI(commandArguments[i]);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI '" + commandArguments[i] + "': " + e.getMessage());
	    		return false;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = Cli.getFileSystem(sourceUri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + sourceUri.getHost());
	    	}
	
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
	    		sourceProvider.copy(sourcePath, destinationPath,
	    			StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING);
	    	} catch (ProviderMismatchException e) {
	    		LOG.debug("Source provider with scheme {} cannot perform copy to {}, falling back to manual copy",
	    				sourceProvider.getScheme(), destinationProvider.getScheme());
		    	int filesCopied = 0;

		    	// Windows FS provider implemenation doesn't let you copy using it as the source
	    		// to another destintation provider implementation like our cloud one, da-da-dumb
	    		if (Files.isDirectory(sourcePath)) {
	    			filesCopied = FileSystemProviderHelper.copyDirectoryBetweenProviders(sourcePath, destinationPath);
	    		} else {
	    			filesCopied = FileSystemProviderHelper.copyFileUsingDestinationProvider(sourcePath, destinationPath);
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
