package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.uk.xarixa.cloud.filesystem.cli.Cli;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.CommandOption;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPathException;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper.DirectoryIterationPaths;
import com.uk.xarixa.cloud.filesystem.core.nio.file.PathFilters;

public class ZipCommand extends AbstractCliCommand {
	private static final Logger LOG = LoggerFactory.getLogger(ZipCommand.class);
	private static final String RECURSIVE_OPTION = "recursive";
	private static final String FILTER_OPTION = "filter";
	private static final List<CommandOption> options =
			Lists.newArrayList(new CommandOption(RECURSIVE_OPTION), new CommandOption(FILTER_OPTION, true, true));

	@Override
	public String getCommandName() {
		return "zip";
	}

	@Override
	public void printSummaryHelp(PrintWriter out) {
		out.println("Zip files to a destination");
	}

	@Override
	public void printFullHelp(PrintWriter out) {
		out.println("Transfer files from a source location into another destination by creating a "
				+ "ZIP file for all of the source entries. The resulting ZIP file contains "
				+ "all of the files from the source location.");
		out.println("\t- Create a zip from the local filesystem 'dir2' and all of it's contents:");
		out.println("\t\tzip --recursive file:///dir1/dir2 cloud://s3-host/container1/dir1/myzip.zip");
		out.println("\t- Create a zip from the local filesystem 'dir2' and 'dir3' all of it's contents:");
		out.println("\t\tzip --recursive file:///dir1/dir2 file:///dir1/dir3 cloud://s3-host/container1/dir1/myzip.zip");
		out.println("\t- Create a zip from the local filesystem 'dir2' and 'dir3' "
				+ "with GLOB-style filtering to add only files ending with '.java':");
		out.println("\t\tzip --recursive --filter=glob:**/*.java file:///dir1/dir3 cloud://s3-host/container1/dir1/myzip.zip");
		out.println("\t- Create a zip from the local filesystem 'dir2' and 'dir3' "
				+ "with REGEX-style filtering to add only files ending with '.java':");
		out.println("\t\tzip --recursive --filter=regex:.*\\.java file:///dir1/dir3 cloud://s3-host/container1/dir1/myzip.zip");
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

    	FileSystemProvider destinationProvider = Cli.getFileSystem(destinationUri).provider();
    	Path destinationPath;
    	try {
    		destinationPath = destinationProvider.getPath(destinationUri);
    	} catch (CloudPathException e) {
    		System.err.println("The cloud path '" + destinationUri + "' is not valid, it must have a container path");
    		return false;
    	}

		final String destinationFsSeparator = destinationPath.getFileSystem().getSeparator();
		
		// Create the ZIP
		ZipOutputStream zipOut = createDestinationZip(destinationPath);

		// Cycle through all of the paths and add them
		try {
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
	
		    	FileSystemProvider sourceProvider = fileSystem.provider();
		    	Path sourcePath;
		    	try {
		    		sourcePath = sourceProvider.getPath(sourceUri);
		    	} catch (CloudPathException e) {
		    		System.err.println("The cloud path '" + destinationUri + "' is not valid, it must have a container path");
		    		return false;
		    	}
	
				final String sourceFsSeparator = sourcePath.getFileSystem().getSeparator();
				Filter<Path> pathFilters =
						createPathFilters(parsedCommand.getCommandOptionsByName(FILTER_OPTION), sourceFsSeparator);

				try {
		    		FileSystemProviderHelper.iterateOverDirectoryContents(
		    				sourcePath.getFileSystem(), Optional.ofNullable(sourcePath), PathFilters.ACCEPT_ALL_FILTER,
		    					recursive, subPath -> {
		    						try {
										return copyToZip(zipOut, sourceFsSeparator, destinationFsSeparator,
												pathFilters, subPath);
									} catch (Exception e) {
										LOG.warn("I/O exception copying '{}' to ZIP", subPath, e);
										return false;
									}
		    					});
		    	} catch (Exception e) {
					System.err.println("Could not create directory '" + sourceUri.toString() + "': " + e.getMessage());
					e.printStackTrace();
		    		return false;
				}
	    	}
		} finally {
			IOUtils.closeQuietly(zipOut);
		}

    	return true;
	}

	protected Boolean copyToZip(ZipOutputStream zipOut, final String sourceFsSeparator,
			final String destinationFsSeparator, Filter<Path> pathFilters,
			DirectoryIterationPaths subPath) throws IOException {
		// Only copy files and not directories
		if (!Files.isDirectory(subPath.getResultPath()) && pathFilters.accept(subPath.getResultPath())) {
			// Create a path to the zip file
			Optional<Path> relativeSourcePath = subPath.getRelativeSourcePath();
			StringBuilder destinationPathString = new StringBuilder();

			// Append the relative path if it exists
			if (relativeSourcePath.isPresent()) {
				if (sourceFsSeparator.equals(destinationFsSeparator)) {
					destinationPathString.append(relativeSourcePath.get().toString())
						.append(destinationFsSeparator);
				} else {
					destinationPathString.append(
							StringUtils.replace(relativeSourcePath.get().toString(),
									sourceFsSeparator, destinationFsSeparator))
						.append(destinationFsSeparator);
				}
			}

			destinationPathString.append(subPath.getResultPath().getFileName());

			// Open the source file
			InputStream sourceInputStream;
			try {
				sourceInputStream = new BufferedInputStream(
						Files.newInputStream(subPath.getResultPath()));
			} catch (Exception e) {
				System.err.println("Could not open the source file '" +
						subPath.getResultPath());
				return false;
			}

			// Transfer the content
			try {
				LOG.debug("Creating ZIP entry '{}' from source file '{}'",
						destinationPathString, subPath.getResultPath());
				zipOut.putNextEntry(new ZipEntry(destinationPathString.toString()));
				IOUtils.copyLarge(sourceInputStream, zipOut);
				zipOut.closeEntry();
			} catch (Exception e) {
				System.err.println("Could not copy source entry '" + subPath.getResultPath() + "'");
				e.printStackTrace();
				return false;
			} finally {
				IOUtils.closeQuietly(sourceInputStream);
			}
		} else {
			LOG.debug("Skipping {} (is file={}, matches filters={})",
					subPath.getResultPath(), !Files.isDirectory(subPath.getResultPath()),
					pathFilters.accept(subPath.getResultPath()));
		}

		return true;
	}

	protected ZipOutputStream createDestinationZip(Path destinationZip) {
		// Create parent directories
		Path parent = destinationZip.getParent();
		if (parent == null) {
			throw new RuntimeException("Expected a path to the ZIP file location, but "
					+ "this ZIP location '" + destinationZip + "' has no parent path");
		}
		LOG.debug("Creating parent directories '{}' for ZIP '{}'", parent, destinationZip);
		FileSystemProviderHelper.ensureDirectoriesExist(parent);

		// Open a file to the destination
		LOG.debug("Creating ZIP output file '{}'", destinationZip);
		ZipOutputStream zipOut;
		try {
			zipOut = new ZipOutputStream(Files.newOutputStream(destinationZip));
		} catch (IOException e) {
    		System.err.println("Could not open a ZIP file to the path '" + destinationZip + "'");
    		throw new RuntimeException("Could not open a ZIP file to the  path '" + destinationZip + "'", e);
		}
		return zipOut;
	}

}
