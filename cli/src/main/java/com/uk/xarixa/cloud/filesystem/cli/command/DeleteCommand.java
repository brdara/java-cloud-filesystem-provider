package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

import com.uk.xarixa.cloud.filesystem.cli.Cli;

/**
 * Deletes one or more containers, directories or files
 */
public class DeleteCommand implements CliCommand {

	@Override
	public String getCommandName() {
		return "delete";
	}

	@Override
	public void printHelp(PrintWriter out) {
		out.println("Deletes one or more files from the filesystem.");
	}

	@Override
	public boolean execute(String[] commandArguments) {
    	if (commandArguments.length < 2) {
    		System.err.println("Not enough arguments");
			return false;
    	}

    	for (int i=1; i<commandArguments.length; i++) {
	    	URI uri;
	    	try {
				uri = new URI(commandArguments[i]);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI: " + e.getMessage());
				return false;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = Cli.getFileSystem(uri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + uri.getHost());
	    	}
	
	    	FileSystemProvider provider = fileSystem.provider();
	    	Path path = provider.getPath(uri);
	    	try {
				if (provider.deleteIfExists(path)) {
					System.out.println("Deleted directory '" + path.toString() + "'");
				} else {
					System.err.println("Could not delete directory '" + path.toString() + "', it may not exist");
				}
			} catch (Exception e) {
				System.err.println("Could not create directory '" + uri.toString() + "': " + e.getMessage());
				e.printStackTrace();
				return false;
			}
    	}
    	
    	return true;
	}

}
