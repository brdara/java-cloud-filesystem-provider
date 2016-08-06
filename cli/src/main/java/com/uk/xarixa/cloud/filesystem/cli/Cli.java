package com.uk.xarixa.cloud.filesystem.cli;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

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
            	String line = reader.readLine("> ");
            	switch (line) {
	            	case "exit":
	            	case "quit":
	            		running.set(false);
            	}
            	
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            running.set(false);
        }
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
