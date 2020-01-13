package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.CommandOption;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.file.PathFilters;
import com.uk.xarixa.cloud.filesystem.core.nio.options.DeleteOption;

/**
 * Deletes one or more containers, directories or files
 */
public class DeleteCommand extends AbstractCliCommand {
	private static final String RECURSIVE_OPTION = "recursive";
	private static final List<CommandOption> options = Lists.newArrayList(new CommandOption(RECURSIVE_OPTION));

	@Override
	public String getCommandName() {
		return "delete";
	}
	
	@Override
	public void printSummaryHelp(PrintWriter out) {
		out.println("Deletes files or directories");
	}

	@Override
	public void printFullHelp(PrintWriter out) {
		out.println("Deletes one or more files from the filesystem.");
		out.println("\t- Delete files on the local filesystem:");
		out.println("\t\tdelete file:///dir1/dir2");
		out.println("\t- Delete dir1 in the container1 on a cloud filesystem mounted as 's3-host':");
		out.println("\t\tdelete cloud://s3-host/container1/dir1");
		out.println("\t- Delete dir1 and all sub-directories and content recursively in the container1 on a cloud filesystem "
				+ "mounted as 's3-host', forcing deletion of all content in the directories:");
		out.println("\t\tdelete --recursive cloud://s3-host/container1/dir1");
	}

	@Override
	public List<CommandOption> getCommandOptions() {
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
		boolean recursive = parsedCommand.getCommandOptionByName(RECURSIVE_OPTION) != null;

		for (String pathString : parsedCommand.getCommandParameters()) {
	    	URI uri;
	    	try {
				uri = new URI(pathString);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI: " + e.getMessage());
				break;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = Cli.getFileSystem(uri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + uri.getHost());
	    	}
	
	    	FileSystemProvider provider = fileSystem.provider();
	    	Path path = provider.getPath(uri);

	    	if (Files.isDirectory(path)) {
	    		
	    		// For a directory work out if sub-dirs and files exist
	    		if (!recursive &&
	    				!FileSystemProviderHelper.iterateOverDirectoryContents(fileSystem, Optional.ofNullable(path),
	    						PathFilters.ACCEPT_ALL_FILTER, recursive,
								subPath -> {
									// Any content in the directory without the recursive option should throw an error
									return false;
								})) {

	    			System.err.println("Cannot delete as sub-directories exist, use the "
							+ "--recursive option to delete sub-directories and content as well");
	    			return false;
	    		}

				return deleteDirectory(provider, path, recursive);
	    	} else {
	    		return deletePath(provider, path);
	    	}
    	}
    	
    	return true;
	}
	
	private boolean deleteDirectory(FileSystemProvider provider, Path dirPath, boolean recursive) {
		// Use the built-in recursive method here for deletion if this is a cloud file system type
		if (provider instanceof CloudFileSystemProvider) {
			try {
				// Perform optimised recursive delete
				if (recursive) {
					((CloudFileSystemProvider)provider).delete((CloudPath)dirPath, DeleteOption.RECURSIVE);
				} else {
					((CloudFileSystemProvider)provider).delete((CloudPath)dirPath);
				}
			} catch (IOException e) {
				System.err.println("Could not delete '" + dirPath.toAbsolutePath() + "' (recursive=" +
						recursive + "): " + e.getMessage());
				e.printStackTrace();
				return false;
			}
		} else {

			// Store the directories with those containing most elements at the top of the set
			SortedSet<Path> directories = new TreeSet<>(new Comparator<Path>() {
				@Override
				public int compare(Path o1, Path o2) {
					if (o1.getParent().equals(o2) || o1.getNameCount() > o2.getNameCount()) {
						return -1;
					}

					if (o2.getParent().equals(o1) || o1.getNameCount() < o2.getNameCount()) {
						return 1;
					}
					
					// Never return 0 unless the paths are equal
					if (o1.equals(o2)) {
						// Duplicate path
						return 0;
					}

					return 1;
				}
			});

			// Iterate across the directories, deleting all content
			if (!FileSystemProviderHelper.iterateOverDirectoryContents(dirPath.getFileSystem(), Optional.ofNullable(dirPath),
				PathFilters.ACCEPT_ALL_FILTER, recursive,
					subPath -> {
						// Save directories for later after recursion has completed
						if (Files.isDirectory(subPath.getResultPath())) {
							directories.add(subPath.getResultPath());
							return true;
						}

						return deletePath(provider, subPath.getResultPath());
					})) {
				// Operation failed to delete paths for some reason
				System.err.println("Could not complete deletion of '" + dirPath.toAbsolutePath() + "'");
			} else {
				directories.add(dirPath);
				System.out.println("Deleting directories: " + directories);

				// Now delete the directories
				for (Path directory : directories) {
					if (!deletePath(provider, directory)) {
						return false;
					}
				}
			}

		}

		return true;
	}

	private boolean deletePath(FileSystemProvider provider, Path path) {
		// Simple file delete
    	try {
			if (provider.deleteIfExists(path)) {
				System.out.println("Deleted path '" + path.toAbsolutePath() + "'");
			} else {
				System.err.println("Could not delete path '" + path.toAbsolutePath() + "', it may not exist");
			}
		} catch (Exception e) {
			System.err.println("Could not delete path '" + path.toAbsolutePath() + "': " + e.getMessage());
			e.printStackTrace();
			return false;
		}
    	
    	return true;
	}

}
