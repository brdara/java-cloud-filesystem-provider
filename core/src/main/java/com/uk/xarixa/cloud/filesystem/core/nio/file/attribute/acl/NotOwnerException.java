package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl;

/**
 * This is an exception that is thrown whenever the modification of an object
 * (such as an Access Control List) is only allowed to be done by an owner of
 * the object, but the Principal attempting the modification is not an owner.
 */
public class NotOwnerException extends Exception {
	private static final long serialVersionUID = -2554352886624101347L;

	/**
     * Constructs a NotOwnerException.
     */
    public NotOwnerException() {
    }
}
