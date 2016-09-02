package com.uk.xarixa.cloud.filesystem.cli;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.security.Principal;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.hash.HashCode;
import com.uk.xarixa.cloud.filesystem.core.host.factory.JCloudsCloudHostProvider;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemProviderDelegate;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPathException;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclEntry;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudFileAttributesView;

/**
 * Simple command-line interface for the filesystem provider
 */
public class Cli {
	private Terminal terminal;
	private AtomicBoolean running = new AtomicBoolean(false);

	Cli(Terminal terminal) {
        this.terminal = terminal;
	}
	
	void run() {
		running.set(true);
		new Thread(this::inputLoop, "Mux input loop").start();
		
		while (running.get()) {
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
			}
		}
	}
	
    private void inputLoop() {
    	LineReader reader = LineReaderBuilder.builder().appName("JCFS").terminal(terminal).build();

    	try {
            while (running.get() ) {
            	try {
	            	String[] arguments = StringUtils.split(StringUtils.trim(reader.readLine("> ")));
	
	            	if (arguments.length > 0) {
		            	if (StringUtils.equals(arguments[0], "exit") || StringUtils.equals(arguments[0], "quit")) {
			            	running.set(false);
		            	} else if (StringUtils.equals(arguments[0], "mount")) {
		            		processCloudHostConfiguration(arguments);
		            	} else if (StringUtils.equals(arguments[0], "list")) {
		            		processListCommand(arguments);
		            	} else if (StringUtils.equals(arguments[0], "mkdir")) {
		            		processMkdirCommand(arguments);
		            	} else if (StringUtils.equals(arguments[0], "delete")) {
		            		processDeleteCommand(arguments);
		            	} else if (StringUtils.equals(arguments[0], "copy")) {
		            		processCopyCommand(arguments);
		            	} else if (StringUtils.equals(arguments[0], "help")) {
		            		processHelpCommand(arguments);
		            	} else {
		            		System.err.println("Command not understood");
		            	}
	            	}
            	} catch (Exception e) {
            		System.err.println("An exception occurred");
            		e.printStackTrace();
            	}
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            running.set(false);
        }
    }

	void processHelpCommand(String[] arguments) {
		if (arguments.length > 1) {
			if (arguments[1].equals("list")) {
				System.out.println("Provides a listing for a container/directory.");
				System.out.println("\t- List files on the local filesystem:");
				System.out.println("\t\tlist file:///dir1/dir2");
				System.out.println("\t- List files on a cloud filesystem mounted as 's3-host':");
				System.out.println("\t\tlist cloud://s3-host/container/dir");
				return;
			} else if (arguments[1].equals("mkdir")) {
				System.out.println("Creates a directory. Parent directories must exist and are not created recursively.");
				System.out.println("\t- Make dir2 as a sub-directory of dir1 on the local filesystem:");
				System.out.println("\t\tmkdir file:///dir1/dir2");
				System.out.println("\t- Make a container1 on a cloud filesystem mounted as 's3-host':");
				System.out.println("\t\tmkdir cloud://s3-host/container1");
				System.out.println("\t- Make dir1 in the container1 on a cloud filesystem mounted as 's3-host':");
				System.out.println("\t\tmkdir cloud://s3-host/container1/dir1");
				return;
			} else if (arguments[1].equals("mount")) {
				System.out.println("Mounts a cloud based filesystem using the specified parameters.");
				System.out.println("\t- Mount an AWS S3 cloud filesystem as 's3-host':");
				System.out.println("\t\tmount s3-host AWS accessKey=XXXXXXXXXXX secretKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
				return;
			}
		}

		System.out.println("Available commands:");
		System.out.println("\tdelete - Delete a container, directory or file");
		System.out.println("\tlist - List files");
		System.out.println("\tmkdir - Create directory");
		System.out.println("\tmount - Mount a cloud filesystem");
		System.out.println("To get further help type 'help [COMMAND]'");
	}

	/**
     * Creates a CloudHostConfiguration:
     * <pre>
     * mount [ALIAS] [CLOUD_TYPE] attribute1=value1 ... attributeN=valueN
     * mount s3-user-1 AWS accessKey=XXXX secretKey=YYYY
     * </pre>
     */
    void processCloudHostConfiguration(String[] arguments) {
    	if (arguments.length < 3) {
    		System.err.println("Not enough arguments");
    		return;
    	}
    	
    	String alias = arguments[1];
    	Map<String,Object> env = new HashMap<>();
    	env.put(JCloudsCloudHostProvider.CLOUD_TYPE_ENV, arguments[2]);

    	for (int i=3; i<arguments.length; i++) {
    		String[] avPair = StringUtils.split(arguments[i], "=");
    		if (avPair.length != 2) {
    			System.err.println("Command line '" + StringUtils.join(arguments,  ' ') +
    					"' contains illegal attribute-value pair at '" + arguments[i] + "'");
    			return;
    		}

    		env.put(avPair[0], avPair[1]);
    	}

    	URI uri;
    	try {
			uri = new URI(CloudFileSystemProviderDelegate.CLOUD_SCHEME, alias, "/", null);
		} catch (URISyntaxException e) {
			System.err.println("Could not create URI for alias '" + alias + "': " + e.getMessage());
			return;
		}

    	try {
			FileSystem fs =
					FileSystemProviderHelper.newFileSystem(uri, env, CloudFileSystemProviderDelegate.class.getClassLoader());
//					FileSystems.newFileSystem(uri, env, CloudFileSystemProviderDelegate.class.getClassLoader());
		} catch (Exception e) {
			System.err.println("Could not create file system for alias '" + alias + "' with env " +
					env.toString() + " : " + e.getMessage());
			return;
		}
    	
    	System.out.println("Cloud filesystem mounted at " + uri.toString());
    }
    
    /**
     * <pre>
     * list [PATH]
     * </pre>
     * @param arguments
     */
    void processListCommand(String[] arguments) {
    	if (arguments.length < 2) {
    		System.err.println("Not enough arguments");
    		return;
    	}

    	URI uri;
    	try {
			uri = new URI(arguments[1]);
		} catch (URISyntaxException e) {
			System.err.println("Could not parse filesystem URI: " + e.getMessage());
			return;
		}

    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
    	FileSystem fileSystem = getFileSystem(uri);
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
					return;
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
	}

	protected FileSystem getFileSystem(URI uri) {
		URI normalisedUri = uri;
		if (!StringUtils.isBlank(uri.getPath()) || !uri.getPath().equals("/")) {
			try {
				normalisedUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
						uri.getPort(), "/", uri.getQuery(), uri.getFragment());
			} catch (URISyntaxException e) {
				System.err.println("Could not normalise URI, will attempt to use existing URI");
			}
		}

		return FileSystemProviderHelper.getFileSystem(normalisedUri);
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

	void processMkdirCommand(String[] arguments) {
    	if (arguments.length < 2) {
    		System.err.println("Not enough arguments");
    		return;
    	}

    	for (int i=1; i<arguments.length; i++) {
	    	URI uri;
	    	try {
				uri = new URI(arguments[i]);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI: " + e.getMessage());
				return;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = getFileSystem(uri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + uri.getHost());
	    	}
	
	    	FileSystemProvider provider = fileSystem.provider();
	    	Path path = provider.getPath(uri);
	    	try {
				provider.createDirectory(path);
				System.out.println("Created directory '" + path.toString() + "'");
			} catch (Exception e) {
				System.err.println("Could not create directory '" + uri.toString() + "': " + e.getMessage());
				e.printStackTrace();
				return;
			}
    	}
	}

	void processDeleteCommand(String[] arguments) {
    	if (arguments.length < 2) {
    		System.err.println("Not enough arguments");
    		return;
    	}

    	for (int i=1; i<arguments.length; i++) {
	    	URI uri;
	    	try {
				uri = new URI(arguments[i]);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI: " + e.getMessage());
				return;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = getFileSystem(uri);
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
				return;
			}
    	}
	}

	void processCopyCommand(String[] arguments) {
    	if (arguments.length < 3) {
    		System.err.println("Not enough arguments");
    		return;
    	}
    	
    	String destination = arguments[arguments.length - 1];
    	URI destinationUri;
    	try {
    		destinationUri = new URI(destination);
		} catch (URISyntaxException e) {
			System.err.println("Could not parse destination filesystem URI '" + destination + "': " + e.getMessage());
			return;
		}

    	for (int i=1; i<arguments.length - 1; i++) {
	    	URI sourceUri;
	    	try {
				sourceUri = new URI(arguments[i]);
			} catch (URISyntaxException e) {
				System.err.println("Could not parse filesystem URI '" + arguments[i] + "': " + e.getMessage());
				return;
			}
	
	    	// TODO: Cannot call FileSystems.getFileSystem because this only works with installed providers
	    	FileSystem fileSystem = getFileSystem(sourceUri);
	    	if (fileSystem == null) {
	    		System.err.println("No file system alias called '" + sourceUri.getHost());
	    	}
	
	    	FileSystemProvider sourceProvider = fileSystem.provider();
	    	Path sourcePath;
	    	try {
	    		sourcePath = sourceProvider.getPath(sourceUri);
	    	} catch (CloudPathException e) {
	    		System.err.println("The cloud path '" + destinationUri + "' is not valid, it must have a container path");
	    		return;
	    	}

	    	FileSystemProvider destinationProvider = getFileSystem(destinationUri).provider();
	    	Path destinationPath;
	    	try {
	    		destinationPath = destinationProvider.getPath(destinationUri);
	    	} catch (CloudPathException e) {
	    		System.err.println("The cloud path '" + destinationUri + "' is not valid, it must have a container path");
	    		return;
	    	}

	    	try {
	    		sourceProvider.copy(sourcePath, destinationPath,
	    			StandardCopyOption.COPY_ATTRIBUTES, StandardCopyOption.REPLACE_EXISTING);
	    	} catch (ProviderMismatchException e) {
	    		// Windows FS provider implemenation doesn't let you copy using it as the source
	    		// to another destintation provider implementation like our cloud one, da-da-dumb
	    		copyFileUsingDestinationProvider(sourcePath, destinationPath);
	    	} catch (Exception e) {
				System.err.println("Could not create directory '" + sourceUri.toString() + "': " + e.getMessage());
				e.printStackTrace();
				return;
			}
    	}
	}

	private void copyFileUsingDestinationProvider(Path sourcePath, Path destinationPath) {
		if (!Files.isRegularFile(sourcePath)) {
			System.err.println("Cannot copy source filesystem directories from '" + sourcePath + "'");
			return;
		}

		// Get the providers
		FileSystemProvider destinationProvider = destinationPath.getFileSystem().provider();
		long readSize = 0L;

		// Open source file for reading
		try (FileChannel readChannel = FileChannel.open(sourcePath,
				EnumSet.of(StandardOpenOption.READ))) {
			readSize = readChannel.size();
			
			// Open destination for writing
			try (SeekableByteChannel targetChannel = destinationProvider.newByteChannel(destinationPath,
						EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
				readChannel.transferTo(0L, readSize, targetChannel);
			} catch (IOException e) {
				System.err.println("Error writing to destination '" + destinationPath + "'");
				e.printStackTrace();
				return;
			}
		} catch (IOException e) {
			System.err.println("Error reading from source '" + sourcePath + "'");
			e.printStackTrace();
			return;
		}
		
		System.out.println("Transferred " + readSize + " bytes from '" + sourcePath + "' to '" + destinationPath + "'");
	}

	public static void main(String[] args) {
		try (Terminal term = TerminalBuilder.builder().type("unix").name("Java FileSystem").build()) {
			System.out.println("Welcome to the Java Cloud FileSystem Provider CLI");
			new Cli(term).run();
			System.out.println("Goodbye!");
		} catch (IOException e) {
			throw new RuntimeException("Cannot build terminal", e);
		}
	}

}
