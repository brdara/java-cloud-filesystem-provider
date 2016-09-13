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
	public void printSummaryHelp(PrintWriter out) {
		out.println("Prints help information");
	}

	@Override
	public void printFullHelp(PrintWriter out) {
		out.println("Prints help information about a command.");
	}

	@Override
	public boolean execute(String[] commandArguments) {
		if (commandArguments.length > 1) {
			for (CliCommand command : commands) {
				if (command.getCommandName().equals(commandArguments[1])) {
					command.printFullHelp(out);
					return true;
				}
			}
		}

		System.out.println("Available commands:");
		for (CliCommand command : commands) {
			out.print("\t");
			out.print(command.getCommandName());
			out.print("\t-\t");
			command.printSummaryHelp(out);
		}
		System.out.println("To get further help type 'help [COMMAND]'");
		return true;
	}

}
