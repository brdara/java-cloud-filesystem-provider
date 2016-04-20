package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.audit;

import java.nio.channels.FileChannel;
import java.nio.file.attribute.FileAttributeView;
import java.util.Set;

/**
 * A versioned audit log which allows for extracting versions of a file
 */
public interface VersionedAuditLog extends AuditLogEntry {

	/**
	 * Identifying tags for this version
	 * @return	Tags or null if no tags are associated with this version
	 */
	Set<String> versionTags();

	/**
	 * Version numbers for this label. The numbers are top-down with most major version first, most minor last,
	 * e.g. a version number 1.3.9 would be represented as an array with: 1, 3, 9.
	 * @return	Version numbers as an array
	 */
	int[] versionNumbers();

	/**
	 * Gets the associated file attributes view for this file version
	 * @param blobStoreContext
	 * @param type
	 * @return
	 * @throws UnsupportedOperationException	If this operation is not supported, meaning that file attribute
	 * 											views for versioned content are not available
	 */
	<V extends FileAttributeView> V getFileAttributeView(Class<V> type);

	/**
	 * Retrieves a read-only {@link FileChannel} with access to this version's file content
	 * @return
	 * @throws UnsupportedOperationException	If this operation is not supported, meaning that version content is not available
	 * @throws VersionContentDeleted			If the content was previously deleted with {@link #deleteVersionContent()}
	 */
	FileChannel getVersionContent() throws UnsupportedOperationException, VersionContentDeleted;

	/**
	 * Deletes this version content. The version information is still available in this audit log
	 * however {@link #getVersionContent()} will throw a {@link VersionContentDeleted} exception
	 * if an attempt is made to retrieve the content.
	 * @return
	 * @throws UnsupportedOperationException	If this operation is not supported, meaning that version content is not available
	 */
	boolean deleteVersionContent() throws UnsupportedOperationException;

}
