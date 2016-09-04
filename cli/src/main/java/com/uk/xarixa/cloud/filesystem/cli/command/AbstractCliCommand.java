package com.uk.xarixa.cloud.filesystem.cli.command;

import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;

public abstract class AbstractCliCommand implements CliCommand {

	public final boolean execute(String[] commandArguments) {
		ParsedCommand parsedCommand = CliCommandHelper.parseCommand(commandArguments, "--");

		// Check parameter bounds
		int numberOfParameters = parsedCommand.getCommandParameters().size();
		if (numberOfParameters < getMinimumNumberOfParameters()) {
			System.err.println("This command accepts a minimum of " + getMinimumNumberOfParameters());
			return false;
		}
		
		if (getMaximumNumberOfParameters() != -1 && numberOfParameters > getMaximumNumberOfParameters()) {
			System.err.println("This command accepts a maximum of " + getMaximumNumberOfParameters());
			return false;
		}
		
		return executeCommand(parsedCommand);
	}
	
	/**
	 * The minimum number of parameters (excluding the command itself)
	 * @return A number greater than 0
	 */
	public abstract int getMinimumNumberOfParameters();
	
	/**
	 * The maximum number of parameters (excluding the command itself)
	 * @return A number of parameters or -1 for infinite parameters
	 */
	public abstract int getMaximumNumberOfParameters();

	/**
	 * Implement this method
	 */
	public abstract boolean executeCommand(ParsedCommand parsedCommand);

}
