package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class TestUserImpl implements UserPrincipal, Comparable<Principal> {
	private final String name;

	public TestUserImpl(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TestUserImpl)) {
			return false;
		}
		
		return ((TestUserImpl)obj).name.equals(name);
	}

	@Override
	public int compareTo(Principal other) {
		if (!(other instanceof TestUserImpl)) {
			return TestUserImpl.class.getName().compareTo(other.getClass().getName());
		}
		
		return ((TestUserImpl)other).name.compareTo(name);
	}
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}