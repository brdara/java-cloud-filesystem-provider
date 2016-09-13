package com.uk.xarixa.cloud.filesystem.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper;

/**
 * Simple command-line interface for the filesystem provider
 */
public class Cli {
	private static final Logger LOG = LoggerFactory.getLogger(Cli.class);
	private Terminal terminal;
	private AtomicBoolean running = new AtomicBoolean(false);
	private final Set<CliCommand> cliCommands = Sets.newHashSet(new ListCommand(),
			new CopyCommand(), new DeleteCommand(), new MkdirCommand(), new MountCommand());

	Cli(Terminal terminal) {
        this.terminal = terminal;
        cliCommands.add(new HelpCommand(new PrintWriter(System.out, true), cliCommands));
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
		// Process the CLI commands
		for (CliCommand cliCommand : cliCommands) {
			if (cliCommand.getCommandName().equals(arguments[0])) {
				if (!cliCommand.execute(arguments)) {
					// Do something on failure of the command
					LOG.warn("Command failed");
				}
				return true;
			}
		}

		return false;
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
		try (Terminal term = TerminalBuilder.builder().type("unix").name("Java FileSystem").build()) {
			System.out.println("Welcome to the Java Cloud FileSystem Provider CLI");
			new Cli(term).run();
			System.out.println("Goodbye!");
		} catch (IOException e) {
			throw new RuntimeException("Cannot build terminal", e);
		}
	}

}
