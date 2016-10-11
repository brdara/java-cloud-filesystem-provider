package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.google.common.collect.Sets;
import com.uk.xarixa.cloud.filesystem.core.nio.file.PathFilters.AggregateAndFilter;
import com.uk.xarixa.cloud.filesystem.core.nio.file.PathFilters.AggregateOrFilter;

@RunWith(BlockJUnit4ClassRunner.class)
public class PathFiltersTest {
	
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery() {{
		setImposteriser(ClassImposteriser.INSTANCE);
	}};

	@Test
	public void testAndFilterLogicallyAndsAllFiltersAndDoesNotAcceptIfOneOfTheFiltersIsFalse() throws IOException {
		Filter<Path> filter1 = context.mock(Filter.class, "filter1");
		Filter<Path> filter2 = context.mock(Filter.class, "filter2");
		Filter<Path> filter3 = context.mock(Filter.class, "filter3");
		Path mockPath = context.mock(Path.class);

		context.checking(new Expectations() {{
			atMost(1).of(filter1).accept(with(any(Path.class))); will(returnValue(true));
			atMost(1).of(filter2).accept(with(any(Path.class))); will(returnValue(true));
			atMost(1).of(filter3).accept(with(any(Path.class))); will(returnValue(false));
		}});

		AggregateAndFilter andFilter = new AggregateAndFilter(Sets.newHashSet(filter1, filter2, filter3));
		Assert.assertFalse(andFilter.accept(mockPath));
	}

	@Test
	public void testAndFilterLogicallyAndsAllFiltersAndAcceptsIfAllOfTheFiltersAreTrue() throws IOException {
		Filter<Path> filter1 = context.mock(Filter.class, "filter1");
		Filter<Path> filter2 = context.mock(Filter.class, "filter2");
		Filter<Path> filter3 = context.mock(Filter.class, "filter3");
		Path mockPath = context.mock(Path.class);

		context.checking(new Expectations() {{
			atMost(1).of(filter1).accept(with(any(Path.class))); will(returnValue(true));
			atMost(1).of(filter2).accept(with(any(Path.class))); will(returnValue(true));
			atMost(1).of(filter3).accept(with(any(Path.class))); will(returnValue(true));
		}});

		AggregateAndFilter andFilter = new AggregateAndFilter(Sets.newHashSet(filter1, filter2, filter3));
		Assert.assertTrue(andFilter.accept(mockPath));
	}

	@Test
	public void testOrFilterLogicallyOrsAllFiltersAndDoesNotAcceptIfAllOfTheFiltersAreFalse() throws IOException {
		Filter<Path> filter1 = context.mock(Filter.class, "filter1");
		Filter<Path> filter2 = context.mock(Filter.class, "filter2");
		Filter<Path> filter3 = context.mock(Filter.class, "filter3");
		Path mockPath = context.mock(Path.class);

		context.checking(new Expectations() {{
			atMost(1).of(filter1).accept(with(any(Path.class))); will(returnValue(false));
			atMost(1).of(filter2).accept(with(any(Path.class))); will(returnValue(false));
			atMost(1).of(filter3).accept(with(any(Path.class))); will(returnValue(false));
		}});

		AggregateOrFilter orFilter = new AggregateOrFilter(Sets.newHashSet(filter1, filter2, filter3));
		Assert.assertFalse(orFilter.accept(mockPath));
	}

	@Test
	public void testOrFilterLogicallyOrsAllFiltersAndAcceptsIfAnyOfTheFiltersAreTrue() throws IOException {
		Filter<Path> filter1 = context.mock(Filter.class, "filter1");
		Filter<Path> filter2 = context.mock(Filter.class, "filter2");
		Filter<Path> filter3 = context.mock(Filter.class, "filter3");
		Path mockPath = context.mock(Path.class);

		context.checking(new Expectations() {{
			atMost(1).of(filter1).accept(with(any(Path.class))); will(returnValue(false));
			atMost(1).of(filter2).accept(with(any(Path.class))); will(returnValue(false));
			atMost(1).of(filter3).accept(with(any(Path.class))); will(returnValue(true));
		}});

		AggregateOrFilter orFilter = new AggregateOrFilter(Sets.newHashSet(filter1, filter2, filter3));
		Assert.assertTrue(orFilter.accept(mockPath));
	}
}
