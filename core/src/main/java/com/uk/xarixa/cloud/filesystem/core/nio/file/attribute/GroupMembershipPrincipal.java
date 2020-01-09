package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.GroupPrincipal;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.Group;

/**
 * A group class which implements both the file {@link GroupPrincipal} and {@link Group}.
 * This allows for membership checks through the {@link Group} methods.
 */
public abstract class GroupMembershipPrincipal implements GroupPrincipal, Group {

}
