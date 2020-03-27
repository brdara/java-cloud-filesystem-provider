package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.uk.xarixa.cloud.filesystem.core.nio.file.FileTreeComparisonEvent.ComparisonSide;

public class CollatingBySideFileTreeComparisonEventHandler implements FileTreeComparisonEventHandler {
	private SortedSet<FileTreeComparisonEvent> bothEvents = new TreeSet<>(new EventPathComparator(ComparisonSide.BOTH));
	private SortedSet<FileTreeComparisonEvent> leftEvents = new TreeSet<>(new EventPathComparator(ComparisonSide.LHS));
	private SortedSet<FileTreeComparisonEvent> rightEvents = new TreeSet<>(new EventPathComparator(ComparisonSide.RHS));
	
	class EventPathComparator implements Comparator<FileTreeComparisonEvent> {		
		private ComparisonSide side;

		EventPathComparator(ComparisonSide side) {
			this.side = side;
		}
 
		@Override
		public int compare(FileTreeComparisonEvent o1, FileTreeComparisonEvent o2) {
			switch (side) {
				case BOTH: 
				case LHS:
					return o1.getLeftPath().compareTo(o2.getLeftPath());
				case RHS:
					return o1.getRightPath().compareTo(o2.getRightPath());
				default:
					throw new RuntimeException("Unknown comparison side: " + side);
			}
		}
		
	}

	@Override
	public void handleEvent(FileTreeComparisonEvent event) {
		switch (event.getSide()) {
			case BOTH:
				bothEvents.add(event); break;
			case LHS:
				leftEvents.add(event); break;
			case RHS:
				rightEvents.add(event); break;
			default:
				throw new RuntimeException("Unknown comparison side: " + event.getSide());
		}
	}
	
	public SortedSet<FileTreeComparisonEvent> getEventsForSide(ComparisonSide side) {
		switch (side) {
			case BOTH:
				return bothEvents;
			case LHS:
				return leftEvents;
			case RHS:
				return rightEvents;
			default:
				throw new RuntimeException("Unknown comparison side: " + side);
		}
	}

}
