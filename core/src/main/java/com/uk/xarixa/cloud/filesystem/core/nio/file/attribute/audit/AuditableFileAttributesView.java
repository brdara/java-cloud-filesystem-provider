package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.audit;

import java.nio.file.attribute.FileAttributeView;
import java.util.List;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudAclFileAttributes;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.CloudBasicFileAttributes;

/**
 * A mixin interface which represents an auditable file attributes view. Extend {@link CloudBasicFileAttributes} and
 * {@link CloudAclFileAttributes} with this interface to enable auditable attributes.
 */
public interface AuditableFileAttributesView<A extends AuditLogEntry> extends FileAttributeView {

	/**
	 * Provides a history of {@link AuditLogEntry} entries for the file
	 * @return
	 */
	List<A> readHistory();

	/**
	 * Appends a history entry with the corresponding {@link AuditLogEntry} entry to the end
	 * of the list
	 * @param auditLogEntry
	 */
	void addHistory(A auditLogEntry);

}
