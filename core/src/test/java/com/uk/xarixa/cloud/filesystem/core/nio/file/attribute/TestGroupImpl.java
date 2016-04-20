package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.security.Principal;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.GroupMembershipPrincipal;

public class TestGroupImpl extends GroupMembershipPrincipal implements Comparable<Principal> {
	private final String name;
	private final Set<Principal> members = new HashSet<>();

	public TestGroupImpl(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TestGroupImpl)) {
			return false;
		}
		
		return ((TestGroupImpl)obj).name.equals(name);
	}


	@Override
	public int compareTo(Principal other) {
		if (!(other instanceof TestGroupImpl)) {
			return TestGroupImpl.class.getName().compareTo(other.getClass().getName());
		}
		
		return ((TestGroupImpl)other).name.compareTo(name);
	}

	@Override
	public boolean addMember(Principal user) {
		return members.add(user);
	}

	@Override
	public boolean removeMember(Principal user) {
		return members.remove(user);
	}

	@Override
	public boolean isMember(Principal member) {
		return members.contains(member);
	}

	@Override
	public Enumeration<? extends Principal> members() {
		return new Enumeration<Principal>() {
			Iterator<Principal> iterator = members.iterator();
			
			@Override
			public boolean hasMoreElements() {
				return iterator.hasNext();
			}

			@Override
			public Principal nextElement() {
				return iterator.next();
			}
		};
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
