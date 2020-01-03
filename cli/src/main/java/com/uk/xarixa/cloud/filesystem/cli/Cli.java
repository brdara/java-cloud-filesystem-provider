package com.uk.xarixa.cloud.filesystem.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.CopyCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.DeleteCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.HelpCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.ListCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.MkdirCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.MountCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.ZipCommand;
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;
import com.uk.xarixa.cloud.filesystem.core.utils.OptimizedPatternMatcher;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Simple command-line interface for the filesystem provider
 */
public class Cli {
	private static final Logger LOG = LoggerFactory.getLogger(Cli.class);

	private static final String SCRIPTS_ARG = "script";
	private static final String PROPERTY_FILE_ARG = "propertyFile";

	private static OptimizedPatternMatcher propertyMatcher = new OptimizedPatternMatcher("\\$\\{([a-zA-Z0-9\\-\\_\\.]+)\\}");
	private static OptimizedPatternMatcher scriptCommentLine = new OptimizedPatternMatcher("^\\s*#");

	private final Set<CliCommand> cliCommands = Sets.newHashSet(new ListCommand(),
			new CopyCommand(), new DeleteCommand(), new MkdirCommand(), new MountCommand(), new ZipCommand());

	private Terminal terminal;
	private AtomicBoolean running = new AtomicBoolean(false);
	private Map<String,Properties> propertyFiles = new LinkedHashMap<>();
	
	Cli(Terminal terminal) {
        this.terminal = terminal;
        cliCommands.add(new HelpCommand(new PrintWriter(System.out, true), cliCommands));
	}
	
	Cli() {
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
	
	void addPropertyFile(String filename, Properties properties) {
		propertyFiles.put(filename, properties);
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
		            	}

		            	if (!processCliCommands(arguments)) {
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

	protected boolean processCliCommands(String[] arguments) {
		arguments = processArguments(arguments);

		// Process the CLI commands
		for (CliCommand cliCommand : cliCommands) {
			if (cliCommand.getCommandName().equals(arguments[0])) {
				if (!cliCommand.execute(arguments)) {
					// Do something on failure of the command
					LOG.warn("Command failed");
				} else {
					return true;
				}
			}
		}

		System.out.println("Unknown command: " + arguments[0]);
		return false;
	}

	private String[] processArguments(String[] arguments) {
		List<String> args = new ArrayList<>();
		
		for (String argument : arguments) {
			String trimmedArg = StringUtils.trim(argument);
			
			if (StringUtils.isNotBlank(trimmedArg)) {

				// Check if it's a property which needs to be substituted
				StringBuilder argStr = new StringBuilder();
				Matcher matcher = propertyMatcher.getMatcher(trimmedArg);
				int lastMatchEnd = 0;

				while (matcher.find()) {
					if (matcher.start() > 0) {
						// Add the string up to this part matched
						argStr.append(StringUtils.substring(trimmedArg, lastMatchEnd, matcher.start()));
					}

					// Save last matched character
					lastMatchEnd = matcher.end();

					// Get the property name and attempt to find the value
					String propertyName = matcher.group(1);
					String propertyValue = System.getProperty(propertyName);

					if (propertyValue == null) {
						LOG.debug("Could not find property value in environment '{}'", propertyName);
					}

					Iterator<Entry<String, Properties>> propFilesIter = propertyFiles.entrySet().iterator();
					while (propertyValue == null && propFilesIter.hasNext()) {
						Map.Entry<String,Properties> propertiesEntry = propFilesIter.next();
						LOG.debug("Searching for property value '{}' in file '{}'", propertyName, propertiesEntry.getKey());
						propertyValue = propertiesEntry.getValue().getProperty(propertyName);
					}

					if (propertyValue == null) {
						throw new RuntimeException("Could not find property value from property named '" + propertyName + "'");
					}
					
					argStr.append(propertyValue);
				}

				// Add in the rest of the string (or all of it if no match found)
				argStr.append(StringUtils.substring(trimmedArg, lastMatchEnd));
				args.add(argStr.toString());
			}
		}
		
		return args.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
	}

	public static FileSystem getFileSystem(URI uri) {
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

	public static void main(String[] args) {
	    ArgumentParser parser = ArgumentParsers.newFor("cloud-cli").build()
				.description("Maestro offloading server application");
	    parser.addArgument("--" + SCRIPTS_ARG).action(Arguments.append()).dest(SCRIPTS_ARG).metavar("FILE")
	    	.help("Specify script files with multiple command line arguments on each line");
	    parser.addArgument("--" + PROPERTY_FILE_ARG).action(Arguments.append()).dest(PROPERTY_FILE_ARG).metavar("FILE")
    		.help("Specify property files to use for interpolating properties in scripts or commands, e.g. accessKey=${awsAccessKey} can load the 'awsAccessKey' property from one of the files");

	    try (Terminal term = TerminalBuilder.builder().type("unix").name("Java FileSystem").build()) {
			System.out.println("Welcome to the Java Cloud FileSystem Provider CLI");
			Cli cli = new Cli(term);
		    executeCommandLineIfExists(cli, args, parser);
		    cli.run();
			System.out.println("Goodbye!");
		} catch (IOException e) {
			throw new RuntimeException("Cannot build terminal", e);
		}
	}

	private static void executeCommandLineIfExists(Cli cli, String[] args, ArgumentParser parser) {
		try {
			Namespace parsedArgs = parser.parseArgs(args);
			loadPropertyFiles(cli, parsedArgs);
			runScripts(cli, parsedArgs);
		} catch (ArgumentParserException e) {
			parser.printHelp(new PrintWriter(System.err));
			exitWithError("Could not parse command-line arguments: " + e.getMessage(), 100);
		}
	}

	private static void loadPropertyFiles(Cli cli, Namespace parsedArgs) {
		@SuppressWarnings("unchecked")
		List<String> propFiles = (List<String>)parsedArgs.get(PROPERTY_FILE_ARG);
		
		if (propFiles != null && !propFiles.isEmpty()) {
			for (String propFilename : propFiles) {
				Properties props = new Properties();
				File propFile = new File(propFilename);
	
				if (!propFile.exists() || !propFile.canRead()) {
					System.err.println("Property file " + propFile + " does not exist or is not readable, ignoring this file");
				}
	
				try {
					props.load(new BufferedReader(new FileReader(propFile)));
					cli.addPropertyFile(propFilename, props);
					LOG.debug("Loaded properties from file {}", propFile.getAbsolutePath());
				} catch (IOException e) {
					System.err.println("Could not read property file " + propFile + ": " + e.getMessage());
				}
			}
		}
	}

	private static void runScripts(Cli cli, Namespace parsedArgs) {
		@SuppressWarnings("unchecked")
		List<String> scripts = (List<String>)parsedArgs.get(SCRIPTS_ARG);

		// If any scripts are found then run them
		if (scripts != null && scripts.size() > 0) {

			for (String script : scripts) {
				System.out.println(">>> Running script: " + script);
				File scriptFile = new File(script);
				if (!scriptFile.exists()) {
					exitWithScriptError(script, "Cannot find script file", 110);
				}

				try {
					List<String> scriptLines = IOUtils.readLines(new BufferedReader(new FileReader(scriptFile)));
					for (int i=0; i<scriptLines.size(); i++) {
						String scriptLine = scriptLines.get(i);

						// If line is not empty and it doesn't match the comment line then exec the command
						if (StringUtils.isNotBlank(scriptLine) && !scriptCommentLine.getMatcher(scriptLine).find()) {

							try {
								if (!cli.processCliCommands(StringUtils.split(scriptLine, " "))) {
									exitWithScriptError(script, i, "Error running CLI command: '" + scriptLine + "'", 112);
								}
							} catch (Exception e) {
								exitWithScriptError(script, i, "Error running CLI command: '" + scriptLine + "': " + e.getMessage(), 113);
							}

						}
					}
				} catch (IOException e) {
					exitWithScriptError(script, "Error reading script file", 111);
				}
				
				System.out.println(">>> Finished running script: " + script);
			}

			System.exit(0);
		}
	}

	public static void exitWithScriptError(String scriptFile, String errorMessage, int exitCode) {
		exitWithScriptError(scriptFile, -1, errorMessage, exitCode);
	}

	public static void exitWithScriptError(String scriptFile, int lineNumber, String errorMessage, int exitCode) {
		StringBuilder errorStr = new StringBuilder(scriptFile);
		if (lineNumber >= 0) {
			errorStr.append(":").append(Integer.toString(lineNumber));
		}

		errorStr.append(":: ")
			.append(errorMessage);
		
		exitWithError(errorStr.toString(), exitCode);
	}

	public static void exitWithError(String errorMessage, int exitCode) {
		System.err.println(errorMessage);
		System.exit(exitCode);
	}

}
