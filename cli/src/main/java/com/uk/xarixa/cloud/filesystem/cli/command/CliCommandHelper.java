package com.uk.xarixa.cloud.filesystem.cli.command;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CliCommandHelper {
	private static final Logger LOG = LoggerFactory.getLogger(CliCommandHelper.class);

	private CliCommandHelper() {
		throw new UnsupportedOperationException();
	}
	
	public final static class ParsedCommand {
		private final List<String> commandOptions = new ArrayList<String>(2);
		private final List<String> commandParameters = new ArrayList<String>(2);

		ParsedCommand(List<String> allowedOptions, String[] arguments, String optionPrefix) {
			boolean inOptions = true;

			for (int i=1; i<arguments.length; i++) {
				String arg = arguments[i];

				if (inOptions) {
					if (arg.startsWith(optionPrefix)) {
						String option = StringUtils.removeStart(arg, optionPrefix);
						if (allowedOptions.contains(option)) {
							commandOptions.add(option);
						} else {
							LOG.warn("Command option {} not recognised, skipping", option);
						}
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

	public final static ParsedCommand parseCommand(List<String> allowedOptions, String[] arguments, String optionPrefix) {
		return new ParsedCommand(allowedOptions, arguments, optionPrefix);
	}

}
