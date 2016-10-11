package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import com.uk.xarixa.cloud.filesystem.core.nio.FileSystemProviderHelper.AcceptAllFilter;

public class PathFilters {
	public static final Filter<Path> ACCEPT_ALL_FILTER = new AcceptAllFilter();

	public static abstract class AggregateFilter implements Filter<Path> {
		final Set<Filter<Path>> filtersSet = new HashSet<>(5);
		
		public AggregateFilter() {
		}

		public AggregateFilter(Set<Filter<Path>> filtersSet) {
			this.filtersSet.addAll(filtersSet);
		}

		public void addAggregateFilter(Filter<Path> pathFilter) {
			filtersSet.add(pathFilter);
		}
		
		boolean checkAccepts(Filter<Path> filter, Path entry) {
			try {
				return filter.accept(entry);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * A filter which can logically AND a set of filters. All filters in the
	 * set must accept the pattern in order for the accept to return true.
	 */
	public static class AggregateAndFilter extends AggregateFilter {
		
		public AggregateAndFilter() {
			super();
		}

		public AggregateAndFilter(Set<Filter<Path>> andFiltersSet) {
			super(andFiltersSet);
		}
		
		@Override
		public boolean accept(Path entry) throws IOException {
			return filtersSet.stream().allMatch(f -> checkAccepts(f, entry));
		}

	}

	/**
	 * A filter which can logically OR across a set of filters. Any filter in the
	 * set must accept the pattern in order for the accept to return true.
	 */
	public static class AggregateOrFilter extends AggregateFilter {
		
		public AggregateOrFilter() {
			super();
		}

		public AggregateOrFilter(Set<Filter<Path>> orFiltersSet) {
			super(orFiltersSet);
		}
		
		@Override
		public boolean accept(Path entry) throws IOException {
			return filtersSet.stream().anyMatch(f -> checkAccepts(f, entry));
		}

	}

}
