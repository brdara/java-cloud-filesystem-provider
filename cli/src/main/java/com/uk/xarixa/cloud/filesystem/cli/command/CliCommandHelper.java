package com.uk.xarixa.cloud.filesystem.cli.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CliCommandHelper {
	private static final Logger LOG = LoggerFactory.getLogger(CliCommandHelper.class);

	private CliCommandHelper() {
		throw new UnsupportedOperationException();
	}
	
	public static class CommandOption {
		private final String name;
		private final boolean hasArgs;
		private final boolean mandatoryArgs;
		
		CommandOption(String name) {
			this(name, false, false);
		}

		CommandOption(String name, boolean hasArgs) {
			this(name, hasArgs, false);
		}

		CommandOption(String name, boolean hasArgs, boolean mandatoryArgs) {
			this.name = name;
			this.hasArgs = hasArgs;
			this.mandatoryArgs = mandatoryArgs;
		}
		
		public String getName() {
			return name;
		}

		public boolean hasArgs() {
			return hasArgs;
		}

		public boolean isMandatoryArgs() {
			return mandatoryArgs;
		}

	}

	public final static class UserCommandOption extends CommandOption {
		private final String value;
		
		UserCommandOption(CommandOption commandOption) {
			this(commandOption.name, commandOption.hasArgs, commandOption.mandatoryArgs, null);
		}

		UserCommandOption(CommandOption commandOption, String argValue) {
			this(commandOption.name, commandOption.hasArgs, commandOption.mandatoryArgs, argValue);
		}

		UserCommandOption(String name, boolean hasArgs, boolean mandatoryArgs, String argValue) {
			super(name, hasArgs, mandatoryArgs);
			this.value = argValue;
		}

		public String getValue() {
			return value;
		}

	}
	

	public final static class ParsedCommand {
		private final List<UserCommandOption> commandOptions = new ArrayList<UserCommandOption>(2);
		private final List<String> commandParameters = new ArrayList<String>(2);

		ParsedCommand(List<CommandOption> allowedOptions, String[] arguments, String optionPrefix) {
			boolean inOptions = true;

			for (int i=1; i<arguments.length; i++) {
				String arg = arguments[i];

				if (inOptions) {
					if (arg.startsWith(optionPrefix)) {
						String option = StringUtils.removeStart(arg, optionPrefix);
						String[] splitOption = StringUtils.split(option, '=');
						if (splitOption.length > 2) {
							LOG.warn("Specified command option is split into more than 2 "
									+ "elements which is not allowed, skipping: {}", Arrays.asList(splitOption));
							continue;
						}

						CommandOption matchingCommandOption = getMatchingCommandOption(allowedOptions, splitOption[0]);
						if (matchingCommandOption != null) {
							
							if (matchingCommandOption.hasArgs && splitOption.length == 2) {
								commandOptions.add(new UserCommandOption(matchingCommandOption, splitOption[1]));
							} else if (matchingCommandOption.hasArgs && matchingCommandOption.mandatoryArgs) {
								// No arg was passed in
								LOG.warn("The command option {} has a mandatory argument which was not "
										+ "supplied, skipping", matchingCommandOption.name);
							} else {
								// Presence of the parameter
								commandOptions.add(new UserCommandOption(matchingCommandOption));
							}
							
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
		
		public CommandOption getMatchingCommandOption(List<CommandOption> allowedOptions, String commandString) {
			for (CommandOption opt : allowedOptions) {
				if (opt.getName().equals(commandString)) {
					return opt;
				}
			}

			return null;
		}

		public List<UserCommandOption> getUserCommandOptions() {
			return commandOptions;
		}

		public UserCommandOption getCommandOptionByName(String name) {
			for (UserCommandOption opt : commandOptions) {
				if (opt.getName().equals(name)) {
					return opt;
				}
			}
			
			return null;
		}

		public List<String> getCommandParameters() {
			return commandParameters;
		}

	}

	public final static ParsedCommand parseCommand(List<CommandOption> allowedOptions,
			String[] arguments, String optionPrefix) {
		return new ParsedCommand(allowedOptions, arguments, optionPrefix);
	}

}
