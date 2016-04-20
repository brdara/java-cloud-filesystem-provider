package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.audit;

import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

/**
 * Models an auditable change in a file
 */
public interface AuditLogEntry {

	/**
	 * The person who made the change
	 * @return
	 */
	UserPrincipal owner();
	
	/**
	 * The date/time of the modification or action
	 * @return
	 */
	FileTime modified();

	/**
	 * File audit actions, the operations which were done during this operation
	 * @return
	 */
	Set<AuditAction> action();

	/**
	 * An associated message/reason for the change
	 * @return
	 */
	String message();

}
