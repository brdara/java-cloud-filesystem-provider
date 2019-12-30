package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl;

/**
 * This is an exception that is thrown whenever an attempt is made to delete
 * the last owner of an Access Control List.
 *
 * @see com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.Owner#deleteOwner
 */
public class LastOwnerException extends Exception {
	private static final long serialVersionUID = 4355058243902851561L;

	/**
     * Constructs a LastOwnerException.
     */
    public LastOwnerException() {
    }
}
