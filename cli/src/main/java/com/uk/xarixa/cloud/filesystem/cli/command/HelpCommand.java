package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.PrintWriter;
import java.util.Set;

public class HelpCommand implements CliCommand {
	private Set<CliCommand> commands;
	private PrintWriter out;

	public HelpCommand(PrintWriter out, Set<CliCommand> commands) {
		this.out = out;
		this.commands = commands;
	}
	
	@Override
	public String getCommandName() {
		return "help";
	}

	@Override
	public void printHelp(PrintWriter out) {
		out.println("Prints help information about a command.");
	}

	@Override
	public boolean execute(String[] commandArguments) {
		if (commandArguments.length > 1) {
			for (CliCommand command : commands) {
				if (command.getCommandName().equals(commandArguments[1])) {
					command.printHelp(out);
					return true;
				}
			}
		}

		System.out.println("Available commands:");
		System.out.println("\tdelete - Delete a container, directory or file");
		System.out.println("\tlist - List files");
		System.out.println("\tmkdir - Create directory");
		System.out.println("\tmount - Mount a cloud filesystem");
		System.out.println("To get further help type 'help [COMMAND]'");
		return true;
	}

}
