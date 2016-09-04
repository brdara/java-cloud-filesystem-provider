package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;

public class MkdirCommand extends AbstractCliCommand {
	private static final Logger LOG = LoggerFactory.getLogger(MkdirCommand.class);

	@Override
	public String getCommandName() {
		return "mkdir";
	}

	@Override
	public void printHelp(PrintWriter out) {
		out.println("Creates a directory. Parent directories must exist and are not created recursively.");
		out.println("\t- Make dir2 as a sub-directory of dir1 on the local filesystem:");
		out.println("\t\tmkdir file:///dir1/dir2");
		out.println("\t- Make a container1 on a cloud filesystem mounted as 's3-host':");
		out.println("\t\tmkdir cloud://s3-host/container1");
		out.println("\t- Make dir1 in the container1 on a cloud filesystem mounted as 's3-host':");
		out.println("\t\tmkdir cloud://s3-host/container1/dir1");
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
		boolean recursive = false;

		for (String commandOption : parsedCommand.getCommandOptions()) {
			if (commandOption.equals("recursive")) {
				recursive = true;
			} else {
				LOG.warn("Command option {} not recognised, skipping", commandOption);
			}
		}

		for (String commandArgument : parsedCommand.getCommandParameters()) {
	    	URI uri;
	    	try {
				uri = new URI(commandArgument);
			} catch (URISyntaxException e) {
				System.err.println("Could not create directory, cannot parse filesystem URI: " + e.getMessage());
	    		break;
			}
	    	
    		// Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = Cli.getFileSystem(uri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + uri.getHost());
	    	}

	    	FileSystemProvider provider = fileSystem.provider();
	    	Path path = provider.getPath(uri);

	    	// Is it a recursive call?
	    	if (recursive) {

	    		if (FileSystemProviderHelper.ensureDirectoriesExist(path)) {
	    			System.out.println("Created all directories for '" + path.toString() + "'");
	    		} else {
	    			System.err.println("Could not create all directories for '" + path.toString() + "'");
	    		}

	    	} else {

		    	try {
					provider.createDirectory(path);
					System.out.println("Created directory '" + path.toString() + "'");
				} catch (Exception e) {
					System.err.println("Could not create directory '" + uri.toString() + "': " + e.getMessage());
					e.printStackTrace();
		    		break;
				}

	    	}
    	}

    	return true;
	}

}
