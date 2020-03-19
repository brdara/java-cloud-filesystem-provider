package com.uk.xarixa.cloud.filesystem.cli.command;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;

import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.CommandOption;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.ParsedCommand;
import com.uk.xarixa.cloud.filesystem.cli.command.CliCommandHelper.UserCommandOption;
import com.uk.xarixa.cloud.filesystem.core.nio.file.PathFilters;
import com.uk.xarixa.cloud.filesystem.core.nio.file.PathFilters.AggregateOrFilter;
import com.uk.xarixa.cloud.filesystem.core.utils.DefaultPathMatcher;

public abstract class AbstractCliCommand implements CliCommand, Comparable<CliCommand> {

	public final boolean execute(String[] commandArguments) {
		ParsedCommand parsedCommand = CliCommandHelper.parseCommand(getCommandOptions(), commandArguments, "--");

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
	 * Gets valid options for the command
	 * @return
	 */
	public abstract List<CommandOption> getCommandOptions();
	
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

	@Override
	public int compareTo(CliCommand other) {
		return getCommandName().compareTo(other.getCommandName());
	}

	/**
	 * Constructs a filter according to the {@link PathMatcher} rules implemented by
	 * {@link DefaultPathMatcher}.
	 * 
	 * @param	pathFilterString
	 * @return	A filter for the pattern
	 * @see		FileSystem#getPathMatcher(String)
	 * @throws	IllegalArgumentException
	 * 				If the syntax and pattern cannot be parsed
	 */
	public Filter<Path> parsePathFilter(String syntaxAndPattern, String fileSystemPathSeparator) throws IllegalArgumentException {
		final DefaultPathMatcher matcher = new DefaultPathMatcher(syntaxAndPattern, fileSystemPathSeparator);

		return new Filter<Path>() {

			@Override
			public boolean accept(Path path) throws IOException {
				return matcher.matches(path);
			}
			
		};
	}

	/**
	 * Creates an {@link AggregateOrFilter} from all of the filters, so that if any of them match
	 * then the path is accepted. Filters are created using {@link #parsePathFilter(String)}.
	 * 
	 * @param commandOptions
	 * @return null if there are no filters created or the {@link AggregateOrFilter}
	 * 
	 * @see #parsePathFilter(String)
	 */
	public Filter<Path> createPathFilters(List<UserCommandOption> commandOptions, String fileSystemPathSeparator) {
		if (commandOptions == null || commandOptions.isEmpty()) {
			return PathFilters.ACCEPT_ALL_FILTER;
		}

		AggregateOrFilter orFilter = new AggregateOrFilter();

		for (UserCommandOption opt : commandOptions) {
			Filter<Path> filter = parsePathFilter(opt.getValue(), fileSystemPathSeparator);
			orFilter.addAggregateFilter(filter);
		}

		return orFilter;
	}

}
