package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;

public interface CliCommand {
	
	/**
	 * Returns the name of this command
	 */
	String getCommandName();

	/**
	 * Prints summary help information (a one-liner) about this command
	 * @param out
	 */
	void printSummaryHelp(PrintWriter out);
	
	/**
	 * Prints detailed help information about this command
	 * @param out
	 */
	void printFullHelp(PrintWriter out);

	/**
	 * Exceutes the command
	 * @param commandArguments
	 * @return	true if the command executed successfully, false otherwise
	 */
	boolean execute(String[] commandArguments);

}
