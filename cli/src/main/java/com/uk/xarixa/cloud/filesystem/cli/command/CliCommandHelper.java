package com.uk.xarixa.cloud.filesystem.cli.command;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public final class CliCommandHelper {

	private CliCommandHelper() {
		throw new UnsupportedOperationException();
	}
	
	public final static class ParsedCommand {
		private final List<String> commandOptions = new ArrayList<String>(2);
		private final List<String> commandParameters = new ArrayList<String>(2);

		ParsedCommand(String[] arguments, String optionPrefix) {
			boolean inOptions = true;

			for (int i=1; i<arguments.length; i++) {
				String arg = arguments[i];

				if (inOptions) {
					if (arg.startsWith(optionPrefix)) {
						commandOptions.add(StringUtils.removeStart(arg, optionPrefix));
					} else {
						inOptions = false;
					}
				}
				
				if (!inOptions) {
					commandParameters.add(arg);
				}
			}
		}

		public List<String> getCommandOptions() {
			return commandOptions;
		}

		public List<String> getCommandParameters() {
			return commandParameters;
		}

	}

	public final static ParsedCommand parseCommand(String[] arguments, String optionPrefix) {
		return new ParsedCommand(arguments, optionPrefix);
	}

}
