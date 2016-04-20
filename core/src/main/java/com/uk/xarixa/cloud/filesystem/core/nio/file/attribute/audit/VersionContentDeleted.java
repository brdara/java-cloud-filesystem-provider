package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.audit;

/**
 * Thrown by {@link VersionedAuditLog#getVersionContent()} where the version content has been
 * deleted.
 */
public class VersionContentDeleted extends RuntimeException {
	private static final long serialVersionUID = -5210300121705296795L;
	private final AuditLogEntry versionDeletedAuditLog;

	public VersionContentDeleted(AuditLogEntry versionDeletedAuditLog) {
		super();
		this.versionDeletedAuditLog = versionDeletedAuditLog;
	}

	public VersionContentDeleted(AuditLogEntry versionDeletedAuditLog, String message,
			Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		this.versionDeletedAuditLog = versionDeletedAuditLog;
	}

	public VersionContentDeleted(AuditLogEntry versionDeletedAuditLog, String message, Throwable cause) {
		super(message, cause);
		this.versionDeletedAuditLog = versionDeletedAuditLog;
	}

	public VersionContentDeleted(AuditLogEntry versionDeletedAuditLog, String message) {
		super(message);
		this.versionDeletedAuditLog = versionDeletedAuditLog;
	}

	public VersionContentDeleted(AuditLogEntry versionDeletedAuditLog, Throwable cause) {
		super(cause);
		this.versionDeletedAuditLog = versionDeletedAuditLog;
	}

	/**
	 * The audit log for this deleted version's content
	 * @return
	 */
	public AuditLogEntry getVersionDeletedAuditLog() {
		return versionDeletedAuditLog;
	}

}
