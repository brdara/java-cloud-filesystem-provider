package com.uk.xarixa.cloud.filesystem.core.nio.file.attribute;

import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.base.Optional;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.LastOwnerException;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.NotOwnerException;
import com.uk.xarixa.cloud.filesystem.core.nio.file.attribute.acl.Owner;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousGroupPrincipal;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipal;
import com.uk.xarixa.cloud.filesystem.core.utils.SafeCloneable;

/**
 * <p>
 * A specialised set for {@link CloudAclEntry}. This uses a {@link ConcurrentSkipListSet} under the hood
 * to allow for concurrent modifications of the set. Conflicts in the set are checked for using an
 * instance of a {@link CloudAclEntryConflictChecker}, the default is {@link DefaultCloudAclEntryConflictChecker#INSTANCE}.
 * <p>
 * <p>
 * There are implicit ACL owners as implemented through the {@link Owner} interface. All operations on the set
 * go through the ACL checks, either one of {@link #checkReadAccess(Principal)}, {@link #checkWriteAccess(Principal)}, or
 * {@link #checkReadWriteAccess(Principal)}. By default all of the ACL owners have read/write permissions for the ACL
 * as implemented in the check methods.
 * </p>
 * 
 * @see CloudAclAdminPermission
 */
public class CloudAclEntrySet implements SafeCloneable<CloudAclEntrySet>, Owner, Iterable<CloudAclEntry<?>> {
	private final Set<Principal> owners = new HashSet<>();
	private final Set<CloudAclEntry<?>> aclSet = new ConcurrentSkipListSet<>();
	private final CloudAclEntryConflictChecker conflictChecker;
	private final ReentrantReadWriteLock ownersLock = new ReentrantReadWriteLock();

	/**
	 * Creates an instance with the {@link DefaultCloudAclEntryConflictChecker#INSTANCE}
	 */
	public CloudAclEntrySet(Principal initialOwner) {
		this(Optional.fromNullable(initialOwner).asSet(), DefaultCloudAclEntryConflictChecker.INSTANCE);
	}

	/**
	 * Creates an instance with the {@link DefaultCloudAclEntryConflictChecker#INSTANCE}
	 */
	public CloudAclEntrySet(Set<Principal> initialOwners) {
		this(initialOwners, DefaultCloudAclEntryConflictChecker.INSTANCE);
	}

	public CloudAclEntrySet(Set<Principal> initialOwners, CloudAclEntryConflictChecker conflictChecker) {
		if (initialOwners == null || initialOwners.isEmpty()) {
			throw new IllegalArgumentException("The initial owner(s) of an ACL cannot be empty/null");
		}

		owners.addAll(initialOwners);
		this.conflictChecker = conflictChecker;
	}

	public CloudAclEntrySet(Principal initialOwner, CloudAclEntryConflictChecker conflictChecker) {
		this(Optional.fromNullable(initialOwner).asSet());
	}

	public CloudAclEntrySet(Principal initialOwner, Set<CloudAclEntry<?>> acls) {
		this(initialOwner);
		try {
			addAllEntries(initialOwner, acls);
		} catch (NotOwnerException e) {
			throw new RuntimeException("Unexpected owner exception in constructor", e);
		}
	}
	
	public CloudAclEntrySet(Set<Principal> initialOwners, Set<CloudAclEntry<?>> acls) {
		this(initialOwners);
		try {
			addAllEntries(initialOwners.stream().findFirst().get(), acls);
		} catch (NotOwnerException e) {
			throw new RuntimeException("Unexpected owner exception in constructor", e);
		}
	}

	public CloudAclEntrySet(Principal initialOwner, CloudAclEntryConflictChecker conflictChecker, Set<CloudAclEntry<?>> acls) {
		this(initialOwner, conflictChecker);
		try {
			addAllEntries(initialOwner, acls);
		} catch (NotOwnerException e) {
			throw new RuntimeException("Unexpected owner exception in constructor", e);
		}
	}

	public CloudAclEntrySet(Set<Principal> initialOwners, CloudAclEntryConflictChecker conflictChecker,
			Set<CloudAclEntry<?>> acls) {
		this(initialOwners, conflictChecker);
		try {
			addAllEntries(initialOwners.stream().findFirst().get(), acls);
		} catch (NotOwnerException e) {
			throw new RuntimeException("Unexpected owner exception in constructor", e);
		}
	}

	public CloudAclEntrySet(Principal initialOwner, CloudAclEntry<?>... acls) {
		this(initialOwner);
		Arrays.stream(acls).forEach(a ->
			{
				try {
					addAclEntry(initialOwner, a);
				} catch (NotOwnerException e) {
					throw new RuntimeException("Unexpected owner exception in constructor", e);
				}
			});
	}

	public CloudAclEntrySet(Principal initialOwner, CloudAclEntryConflictChecker conflictChecker, CloudAclEntry<?>... acls) {
		this(initialOwner, conflictChecker);
		Arrays.stream(acls).forEach(a ->
			{
				try {
					addAclEntry(initialOwner, a);
				} catch (NotOwnerException e) {
					throw new RuntimeException("Unexpected owner exception in constructor", e);
				}
			});
	}

	public int size() {
		return aclSet.size();
	}

	public boolean isEmpty() {
		return aclSet.isEmpty();
	}

	public boolean contains(CloudAclEntry<?> entry) {
		return aclSet.contains(entry);
	}

	/**
	 * Returns a modifiable iterator over the entries in the set
	 * @param caller
	 * @return
	 * @throws NotOwnerException 
	 */
	public Iterator<CloudAclEntry<?>> iterator(Principal caller) throws NotOwnerException {
		checkReadWriteAccess(caller);
		return aclSet.iterator();
	}

	/**
	 * Returns an unmodifiable iterator across the entries in this ACL set
	 * @return
	 */
	public Iterator<CloudAclEntry<?>> iterator() {
		return Collections.unmodifiableSet(getClonedAclSet()).iterator();
	}

	/**
	 * Retrieves the ACL entries as an immutable stream
	 * @return
	 * @throws NotOwnerException 
	 */
	public Stream<CloudAclEntry<?>> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

	/**
	 * Retrieves the ACL entries as a stream which can work on a modified set
	 * @return
	 * @throws NotOwnerException 
	 */
	public Stream<CloudAclEntry<?>> stream(Principal caller) throws NotOwnerException {
		checkReadWriteAccess(caller);
        return StreamSupport.stream(spliterator(caller), false);
    }

	/**
	 * Retrieves the ACL entries as a {@link Spliterator} which can work on a modified set
	 * @param caller
	 * @return
	 * @throws NotOwnerException
	 */
	public Spliterator<CloudAclEntry<?>> spliterator(Principal caller) throws NotOwnerException {
		checkReadWriteAccess(caller);
        return Spliterators.spliteratorUnknownSize(iterator(caller), 0);
    }

	public Object[] toArray() {
		return aclSet.toArray();
	}

	public <T> T[] toArray(T[] a) {
		return aclSet.toArray(a);
	}

	/**
	 * @throws NotOwnerException 
	 * @see #addAclEntry(CloudAclEntry)
	 */
	public boolean addEntry(Principal caller, CloudAclEntry<?> entry) throws NotOwnerException {
		return addAclEntry(caller, entry).isEmpty();
	}

	public boolean removeEntry(Principal caller, CloudAclEntry<?> entry) throws NotOwnerException {
		checkWriteAccess(caller);
		return aclSet.remove(entry);
	}

	public boolean containsAll(Collection<CloudAclEntry<?>> entryCollection) {
		return aclSet.containsAll(entryCollection);
	}

	public boolean addAllEntries(Principal caller, Collection<? extends CloudAclEntry<?>> aclEntryCollection) throws NotOwnerException {
		boolean allAdded = true;

		for (CloudAclEntry<?> entry : aclEntryCollection) {
			if (!addEntry(caller, entry)) {
				allAdded = false;
			}
		}

		return allAdded;
	}

	public boolean removeAll(Principal caller, Collection<?> c) throws NotOwnerException {
		checkWriteAccess(caller);
		return aclSet.removeAll(c);
	}

	public void clear(Principal caller) throws NotOwnerException {
		checkWriteAccess(caller);
		aclSet.clear();
	}

	protected void checkReadAccess(Principal caller) throws NotOwnerException {
		checkAccess(caller, AclConstants.READ_ACL_PERMISSIONS);
	}

	protected void checkWriteAccess(Principal caller) throws NotOwnerException {
		checkAccess(caller, AclConstants.WRITE_ACL_PERMISSIONS);
	}

	protected void checkReadWriteAccess(Principal caller) throws NotOwnerException {
		checkAccess(caller, AclConstants.READ_WRITE_ACL_PERMISSIONS);
	}

	/**
	 * Checks if the caller is an owner or has read/write ACL access. Rather than implement this in
	 * a heavyweight security manager we can just use local checks.
	 * @param caller
	 * @throws NotOwnerException
	 */
	protected void checkAccess(Principal caller, Set<AclEntryPermission> aclPermissions) throws NotOwnerException {
		if (!isOwner(caller) && findAclsOfTypeWithAllPermissions(caller, AclEntryType.ALLOW, aclPermissions).isEmpty()) {
			throw new NotOwnerException();
		}
	}

	@SuppressWarnings("unchecked")
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (!(o instanceof CloudAclEntrySet)) {
			return false;
		}

		CloudAclEntrySet other = (CloudAclEntrySet)o;
		ownersLock.readLock().lock();
		
		try {
			other.ownersLock.readLock().lock();
			
			try {
				Optional<Set<Principal>> myOwners = Optional.fromNullable(owners);
				Optional<Set<Principal>> otherOwners = Optional.fromNullable(other.owners);
				Optional<Set<CloudAclEntry<?>>> myAclSet = Optional.fromNullable(aclSet);
				Optional<Set<CloudAclEntry<?>>> otherAclSet = Optional.fromNullable(other.aclSet);

				return myAclSet.or(Collections.EMPTY_SET).containsAll(otherAclSet.or(Collections.EMPTY_SET)) &&
						myOwners.or(Collections.EMPTY_SET).containsAll(otherOwners.or(Collections.EMPTY_SET));
			} finally {
				other.ownersLock.readLock().unlock();
			}
		} finally {
			ownersLock.readLock().unlock();
		}
	}

	public int hashCode() {
		ownersLock.readLock().lock();
		
		try {
			return new HashCodeBuilder().append(aclSet).append(owners).toHashCode();
		} finally {
			ownersLock.readLock().unlock();
		}
	}
	
	/**
	 * Adds the ACL entry, determining if there are any conflicts and refusing to add to the set if
	 * there were. This is the same as {@link #addAclEntry(CloudAclEntry, boolean) addAclEntry(aclEntry, false)}.
	 * @param aclEntry
	 * @return The conflicting ACL entries if this ACL could not be added, or an empty set
	 * @throws NotOwnerException 
	 * @see {@link #addAclEntry(Principal, CloudAclEntry, boolean)}
	 * @see #findConflictingAcls(CloudAclEntry)
	 */
	public Set<CloudAclEntry<?>> addAclEntry(Principal caller, CloudAclEntry<?> aclEntry) throws NotOwnerException {
		return addAclEntry(caller, aclEntry, false);
	}

	/**
	 * Adds the ACL entry, determining if there are any conflicts and refusing to add to the set if
	 * there were
	 * @param aclEntry
	 * @param force true if you would like to add this ACL and remove any conflicting ACL, false otherwise
	 * @return If <em>force</em> was false then this returns the conflicting ACL entries and this ACL is not added.
	 * 			if <em>force</em> was true then this returns the conflicting ACL entries which were replaced by this
	 * 			new ACL entry.
	 * @throws NotOwnerException 
	 * @see #findConflictingAcls(CloudAclEntry)
	 */
	public Set<CloudAclEntry<?>> addAclEntry(Principal caller, CloudAclEntry<?> aclEntry, boolean force) throws NotOwnerException {
		checkWriteAccess(caller);
		Set<CloudAclEntry<?>> conflictingAclEntries = findConflictingAcls(aclEntry);

		if (force) {
			// TODO: This could cause a race condition where we clear the conflicts and then add,
			// whilst another thread could add another conflicting ACL entry. Consider using a R/W lock
			// for all add/delete operations as for the owners set.
			if (!conflictingAclEntries.isEmpty()) {
				aclSet.removeAll(conflictingAclEntries);
			}

			aclSet.add(aclEntry);
		} else if (conflictingAclEntries.isEmpty()) {
			aclSet.add(aclEntry);
		}

		return conflictingAclEntries;
	}

	/**
	 * Finds any conflicting ACL's, that is if you tried to add this ACL to the set then it
	 * would cause a conflict. This is simply determined by checking each entry in the set where
	 * {@link CloudAclEntry#getPrincipalClass()} is the same for the <em>other</em> entry and
	 * some entry in the set.
	 * @param other
	 * @return
	 */
	public Set<CloudAclEntry<?>> findConflictingAcls(CloudAclEntry<?> other) {
		return findAcls(a -> conflictChecker.isConflictingAcl(a, other));
	}

	@Override
	public String toString() {
		return aclSet.toString();
	}
	

	/**
	 * Returns the ACL's with this class type
	 * @param clazz One of {@link UserPrincipal}, {@link GroupPrincipal}, {@link PublicPrivateCloudPermissionsPrincipal}
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T extends Principal> Set<CloudAclEntry<T>> findAclsWithPrincipalType(Class<T> clazz) {
		boolean isUserPrincipal = UserPrincipal.class.isAssignableFrom(clazz) && !GroupPrincipal.class.isAssignableFrom(clazz);
		return (Set)findAcls(
				a -> clazz.isAssignableFrom(a.getPrincipalClass()) &&
					(!isUserPrincipal || !GroupPrincipal.class.isAssignableFrom(a.getPrincipalClass())));
	}

	/**
	 * Searches for ACL's based on the predicate
	 * @param searchPredicate
	 * @return A set containing the subset of ACL's matcing the predicate
	 */
	public Set<CloudAclEntry<?>> findAcls(Predicate<CloudAclEntry<?>> searchPredicate) {
		return aclSet.stream().filter(searchPredicate).collect(Collectors.toSet());
	}
	
	/**
	 * Finds all ACL's with any of the specified type and with <em>all</em> of the permissions
	 * type.
	 * @param aclOwner
	 * @param type
	 * @return
	 */
	public Set<CloudAclEntry<?>> findAclsOfTypeWithAllPermissions(Principal aclOwner, AclEntryType type,
			Set<AclEntryPermission> permissions) {
		return findAcls(a ->
			type.equals(a.getType()) &&
			aclOwner.equals(a.getPrincipal()) &&
			SetUtils.difference(permissions, a.getPermissions()).isEmpty());
	}

	/**
	 * Finds all ACL's with any of the specified type and with <em>any</em> of the permissions
	 * type.
	 * @param aclOwner
	 * @param type
	 * @return
	 */
	public Set<CloudAclEntry<?>> findAclsOfTypeWithAnyPermissions(Principal aclOwner, AclEntryType type,
			Set<AclEntryPermission> permissions) {
		return findAcls(a ->
			type.equals(a.getType()) &&
			aclOwner.equals(a.getPrincipal()) &&
			SetUtils.difference(permissions, a.getPermissions()).size() < permissions.size());
	}

	/**
	 * Clones this {@link CloudAclEntrySet} and all entries in the set
	 */
	@Override
	public CloudAclEntrySet clone() {
		Set<CloudAclEntry<?>> newAclSet = aclSet.stream().map(a -> a.clone()).collect(Collectors.toSet());
		return new CloudAclEntrySet(owners, conflictChecker, newAclSet);
	}

	@Override
	public boolean addOwner(Principal caller, Principal owner) throws NotOwnerException {
		ownersLock.readLock().lock();
		
		try {
			checkWriteAccess(caller);
			
			// Upgrade to a write lock
			ownersLock.readLock().unlock();
			ownersLock.writeLock().lock();
			boolean returnValue;

			try {
				returnValue = owners.add(owner);

				// Downgrade back to a read lock
				ownersLock.readLock().lock();
			} finally {
				ownersLock.writeLock().unlock();
			}
			
			return returnValue;
		} finally {
			ownersLock.readLock().unlock();
		}
	}

	@Override
	public boolean deleteOwner(Principal caller, Principal owner) throws NotOwnerException, LastOwnerException {
		ownersLock.readLock().lock();
		
		try {
			checkWriteAccess(caller);
			
			// Upgrade to a write lock
			ownersLock.readLock().unlock();
			ownersLock.writeLock().lock();
			boolean returnValue = false;

			try {
				// Cannot delete if this would violate the size constraint
				if (owners.contains(owner)) {
					if (owners.size() == 1) {
						throw new LastOwnerException();
					}
					
					returnValue = owners.remove(owner);
				}
			} finally {
				// Downgrade back to a read lock
				ownersLock.readLock().lock();
				ownersLock.writeLock().unlock();
			}
			
			return returnValue;
		} finally {
			ownersLock.readLock().unlock();
		}
	}

	/**
	 * In the case of {@link AnonymousUserPrincipal} or {@link AnonymousGroupPrincipal} being in the ownership
	 * set then this always returns true. This does not check if the owner is a user of a group which
	 * has ownership, the call should be invoked for the user and all groups of the user until it returns
	 * true if you want to look at the user's groups for ownership, and the user's group should be used for
	 * all updates to the set.
	 */
	@Override
	public boolean isOwner(Principal owner) {
		ownersLock.readLock().lock();
		
		try {
			return owners.contains(owner) || hasAnonymousOwner();
		} finally {
			ownersLock.readLock().unlock();
		}
	}

	/**
	 * True if this ACL set has anonymous ownership
	 * @return
	 * @see	AnonymousUserPrincipal
	 * @see AnonymousGroupPrincipal
	 */
	public boolean hasAnonymousOwner() {
		ownersLock.readLock().lock();
		
		try {
			return owners.stream().anyMatch(o ->
				AnonymousUserPrincipal.class.isAssignableFrom(o.getClass()) ||
					AnonymousGroupPrincipal.class.isAssignableFrom(o.getClass()));
		} finally {
			ownersLock.readLock().unlock();
		}
	}

	/**
	 * Retrieves the owners as an immutable set.
	 */
	public Set<Principal> getOwners() {
		ownersLock.readLock().lock();

		try {
           	return Collections.unmodifiableSet(owners); 
		} finally {
			ownersLock.readLock().unlock();
		}
	}

	/**
	 * Gets all ACL entries.
	 * @return
	 */
	public Set<CloudAclEntry<?>> getAclEntries() {
		return Collections.unmodifiableSet(getClonedAclSet());
	}

	private Set<CloudAclEntry<?>> getClonedAclSet() {
		return aclSet.stream().map(a -> a.clone()).collect(Collectors.toSet());
	}
	
	/**
	 * Invokes {@link CloudAclEntryConflictChecker#mergeAcls(Principal, CloudAclEntrySet)}
	 * with the first owner of the set. Also any entries with empty permissions
	 * or an empty type are removed.
	 */
	public void optimise() {
		// Get the owners read lock so we can add/remove with one owner
		ownersLock.readLock().lock();
		
		try {
			Principal owner = owners.stream().findFirst().get();
			conflictChecker.mergeAcls(owner, this);
		} finally {
			ownersLock.readLock().unlock();
		}
	}

}