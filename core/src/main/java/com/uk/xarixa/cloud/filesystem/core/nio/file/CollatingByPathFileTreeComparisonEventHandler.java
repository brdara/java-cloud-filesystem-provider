package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollatingByPathFileTreeComparisonEventHandler implements FileTreeComparisonEventHandler {
	private final static Logger LOG = LoggerFactory.getLogger(CollatingByPathFileTreeComparisonEventHandler.class);
	private final SortedSet<FileTreeComparisonEvent> sortedEvents = new TreeSet<>(new EventPathComparator());
	private final Set<Kind<?>> kinds;

	class EventPathComparator implements Comparator<FileTreeComparisonEvent> {		
		@Override
		public int compare(FileTreeComparisonEvent o1, FileTreeComparisonEvent o2) {
			Path o1Path = getPathForSide(o1);
			Path o2Path = getPathForSide(o2);
			return o1Path.compareTo(o2Path);
		}
		
		private Path getPathForSide(FileTreeComparisonEvent ev) {
			switch (ev.getSide()) {
				case BOTH: 
				case LHS:
					return ev.getLeftPath();
				case RHS:
					return ev.getRightPath();
				default:
					throw new RuntimeException("Unknown comparison side: " + ev.getSide());
			}
		}
		
	}

	public CollatingByPathFileTreeComparisonEventHandler() {
		this(Collections.emptySet());
	}

	public CollatingByPathFileTreeComparisonEventHandler(Set<Kind<?>> kinds) {
		this.kinds = kinds;
	}

	@Override
	public void handleEvent(FileTreeComparisonEvent event) {
		boolean handleEvent = kinds.isEmpty();

		if (!handleEvent) {
			Kind<?> kind = FileHierarchyHelper.fileTreeComparisonEventToWatchEventKind(event);
			handleEvent = kinds.contains(kind);
		}

		if (handleEvent) {
			sortedEvents.add(event);
		} else {
			LOG.debug("Event discarded because of kinds {}: {}", kinds, event);
		}
	}

	public SortedSet<FileTreeComparisonEvent> getEvents() {
		return sortedEvents;
	}

}
