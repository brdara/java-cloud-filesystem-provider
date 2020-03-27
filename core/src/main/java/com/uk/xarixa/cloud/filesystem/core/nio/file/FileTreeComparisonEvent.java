package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.nio.file.Path;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.google.common.base.Objects;

public class FileTreeComparisonEvent {
	private final ComparisonSide side;
	private final ComparisonResult result;
	private final Path leftPath;
	private final Path rightPath;

	public static enum ComparisonSide {
		BOTH, LHS, RHS
	}
	
	public static enum ComparisonResult {
		/**
		 * File/directory is missing on the other side of the comparison
		 */
		MISSING_OTHER_SIDE,

		/**
		 * Directory is missing children on this side of the comparison, meaning the other side has a non-empty directory
		 * wheras this side it is empty
		 */
		MISSING_CHILDREN,

		/**
		 * File digest is different, content mismatch
		 */
		FILE_DIGEST_MISMATCH,

		/**
		 * This exists as a file in this side, as a directory in the other
		 */
		DIRECTORY_IS_FILE_MISMATCH
	}
	
	public FileTreeComparisonEvent(Path leftPath, Path rightPath, ComparisonSide side, ComparisonResult result) {
		this.rightPath = rightPath;
		this.leftPath = leftPath;
		this.side = side;
		this.result = result;
	}

	public ComparisonSide getSide() {
		return side;
	}

	public ComparisonResult getResult() {
		return result;
	}
	
	public Path getLeftPath() {
		return leftPath;
	}

	public Path getRightPath() {
		return rightPath;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (!(obj instanceof FileTreeComparisonEvent)) {
			return false;
		}
		
		FileTreeComparisonEvent other = (FileTreeComparisonEvent)obj;
		return Objects.equal(side, other.side) &&
				Objects.equal(result, other.result) &&
				Objects.equal(leftPath, other.leftPath) &&
				Objects.equal(rightPath, other.rightPath);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
				.append(side)
				.append(result)
				.append(leftPath)
				.append(rightPath)
				.toHashCode();
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
