package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.nio.DefaultCloudFileSystemImplementation;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchService;
import com.uk.xarixa.cloud.filesystem.core.nio.file.CloudWatchServiceFactory;
import com.uk.xarixa.cloud.filesystem.core.nio.file.DefaultCloudWatchServiceConfiguration;
import com.uk.xarixa.cloud.filesystem.core.security.AnonymousUserPrincipalService;
import com.uk.xarixa.cloud.filesystem.core.security.CloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.DeferringCloudHostSecurityManager;
import com.uk.xarixa.cloud.filesystem.core.security.UserGroupLookupService;

public abstract class AbstractDefaultCloudHostConfiguration extends AbstractCloudHostConfiguration {
	private final static Logger LOG = LoggerFactory.getLogger(AbstractDefaultCloudHostConfiguration.class);
	public static final long DEFAULT_WATCH_SERVICE_CLOUD_POLL_TIME_MS = 120000;
	private static final CloudFileSystemImplementation defaultCloudFileSystemImplementation =
			new DefaultCloudFileSystemImplementation();
	private static final CloudHostSecurityManager defaultCloudHostSecurityManager = new DeferringCloudHostSecurityManager();
	private static final UserPrincipalLookupService defaultUserPrincipalLookupService = new AnonymousUserPrincipalService();
	private Optional<CloudFileSystemImplementation> cloudFileSystemImplementation = Optional.ofNullable(null);
	private Optional<UserPrincipalLookupService> userPrincipalLookupService = Optional.ofNullable(null);
	private Optional<CloudWatchServiceFactory> watchServiceFactory = Optional.ofNullable(null);
	private Optional<CloudHostSecurityManager> cloudHostSecurityManager = Optional.ofNullable(null);
	private long watchServiceCloudPollTimeMs = DEFAULT_WATCH_SERVICE_CLOUD_POLL_TIME_MS;

	/**
	 * This simple implementation tests if the cloud host settings are {@link #equals(Object) equivalent} and returns true if so.
	 * More complex methods can be used to work out if optimised operations can be used between the two paths.
	 */
	@Override
	public boolean canOptimiseOperationsFor(CloudPath cloudPath) {
		return equals(cloudPath.getFileSystem().getCloudHostConfiguration());
	}

	
	private Class<?> getClassForName(String className) {
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Cannot find a class with name " + className);
		}
	}

	private <T extends Object> T createInstanceFromNoArgConstructor(Class<T> targetClass, Class<?> instantiableClass) {
		if (!targetClass.isAssignableFrom(instantiableClass)) {
			throw new IllegalArgumentException("The class " + instantiableClass + " is not an implementation of " +
					targetClass);
		}
		
		// Create an instance
		try {
			LOG.debug("Creating target class {} of target type {}", instantiableClass, targetClass);
			Constructor<?> constructor = instantiableClass.getConstructor();
			return targetClass.cast(constructor.newInstance());
		} catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalArgumentException("Cannot find a no-arg constructor for the class " + instantiableClass);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new IllegalStateException("Cannot instantiate class " + instantiableClass +
					" with no-arg default constructor", e);
		}
	}

	public void setCloudFileSystemImplementationClassName(String className) {
		setCloudFileSystemImplementationClass(getClassForName(className));
	}
	
	public void setCloudFileSystemImplementationClass(Class<?> clazz) {
		setCloudFileSystemImplementation(createInstanceFromNoArgConstructor(CloudFileSystemImplementation.class, clazz));
	}
	
	public void setCloudFileSystemImplementation(CloudFileSystemImplementation cloudFileSystemImplementation) {
		this.cloudFileSystemImplementation = Optional.ofNullable(cloudFileSystemImplementation);
	}

	/**
	 * Implementors should override this to give a default value for the cloud file system implementation
	 * in case one of the setters hasn't been used
	 * @see #getCloudFileSystemImplementation()
	 * @see #setCloudFileSystemImplementation(CloudFileSystemImplementation)
	 * @see #setCloudFileSystemImplementationClass(Class)
	 * @see #setCloudFileSystemImplementationClassName(String)
	 * @return	{@link DefaultCloudFileSystemImplementation}
	 */
	protected CloudFileSystemImplementation getDefaultCloudFileSystemImplementation() {
		return defaultCloudFileSystemImplementation;
	}

	@Override
	public final CloudFileSystemImplementation getCloudFileSystemImplementation() {
		return cloudFileSystemImplementation.orElse(getDefaultCloudFileSystemImplementation());
	}

	public void setUserPrincipalLookupService(String className) {
		setUserPrincipalLookupService(getClassForName(className));
	}
	
	public void setUserPrincipalLookupService(Class<?> clazz) {
		setUserPrincipalLookupService(createInstanceFromNoArgConstructor(UserPrincipalLookupService.class, clazz));
	}

	public void setUserPrincipalLookupService(UserPrincipalLookupService userPrincipalLookupService) {
		this.userPrincipalLookupService = Optional.ofNullable(userPrincipalLookupService);
	}

	/**
	 * Implementors should override this to give a default value for the user principal lookup service
	 * in case one of the setters hasn't been used.
	 * @see #getUserPrincipalLookupService()
	 * @see #setUserPrincipalLookupService(UserPrincipalLookupService)
	 * @see #setUserPrincipalLookupService(Class)
	 * @see #setUserPrincipalLookupService(String)
	 * @return	Returns an {@link AnonymousUserPrincipalService}, implement this method to return a different
	 * default value for this service
	 */
	protected UserPrincipalLookupService getDefaultUserPrincipalLookupService() {
		return defaultUserPrincipalLookupService;
	}

	@Override
	public final UserPrincipalLookupService getUserPrincipalLookupService() {
		return userPrincipalLookupService.orElse(getDefaultUserPrincipalLookupService());
	}

	/**
	 * Returns the {@link #getUserPrincipalLookupService()} cast as a {@link UserGroupLookupService}
	 */
	@Override
	public UserGroupLookupService<?> getUserGroupLookupService() {
		if (getUserPrincipalLookupService() instanceof UserGroupLookupService) {
			return (UserGroupLookupService<?>)getUserPrincipalLookupService();
		}
		
		return null;
	}

	public void setWatchServiceFactory(String className) {
		setWatchServiceFactory(getClassForName(className));
	}
	
	public void setWatchServiceFactory(Class<?> clazz) {
		setWatchServiceFactory(createInstanceFromNoArgConstructor(CloudWatchServiceFactory.class, clazz));
	}
	
	public void setWatchServiceFactory(CloudWatchServiceFactory watchService) {
		this.watchServiceFactory = Optional.ofNullable(watchService);
	}

	/**
	 * Implementors should override this to give a default value for the watch service
	 * in case one of the setters hasn't been used.
	 * @see #createWatchService()
	 * @see #setWatchServiceFactory(CloudWatchServiceFactory)
	 * @see #setWatchServiceFactory(Class)
	 * @see #setWatchServiceFactory(String)
	 * @return	Returns null, implement this method to return a default value for this service
	 */
	protected CloudWatchServiceFactory getDefaultWatchServiceFactory() {
		return null;
	}

	/**
	 * If not set this defaults to {@link #DEFAULT_WATCH_SERVICE_CLOUD_POLL_TIME_MS}
	 */
	@Override
	public void setWatchServiceCloudPollTime(long watchServiceCloudPollTimeMs) {
		this.watchServiceCloudPollTimeMs = watchServiceCloudPollTimeMs;
	}

	/**
	 * @see	#setWatchServiceCloudPollTime(long)
	 * @see #setWatchServiceFactory(CloudWatchServiceFactory)
	 * @see #setWatchServiceFactory(Class)
	 * @see #setWatchServiceFactory(String)
	 */
	@Override
	public final CloudWatchService createWatchService() {
		CloudWatchServiceFactory factory = watchServiceFactory.orElse(getDefaultWatchServiceFactory());
		
		if (factory == null) {
			return null;
		}

		return factory.createWatchService(new DefaultCloudWatchServiceConfiguration(watchServiceCloudPollTimeMs));
	}
	
	public void setCloudHostSecurityManagerClassName(String className) {
		setCloudHostSecurityManagerClass(getClassForName(className));
	}
	
	public void setCloudHostSecurityManagerClass(Class<?> clazz) {
		setCloudHostSecurityManager(createInstanceFromNoArgConstructor(CloudHostSecurityManager.class, clazz));
	}
	
	public void setCloudHostSecurityManager(CloudHostSecurityManager cloudHostSecurityManager) {
		this.cloudHostSecurityManager = Optional.ofNullable(cloudHostSecurityManager);
	}

	/**
	 * Implementors should override this to give a default value for the cloud host security manager
	 * in case one of the setters hasn't been used.
	 * 
	 * @see #setCloudHostSecurityManager(CloudHostSecurityManager)
	 * @see #setCloudHostSecurityManagerClass(Class)
	 * @see #setCloudHostSecurityManagerClassName(String)
	 * @return	{@link DeferringCloudHostSecurityManager}
	 */
	protected CloudHostSecurityManager getDefaultCloudHostSecurityManager() {
		return defaultCloudHostSecurityManager;
	}

	@Override
	public final CloudHostSecurityManager getCloudHostSecurityManager() {
		return cloudHostSecurityManager.orElse(getDefaultCloudHostSecurityManager());
	}

}
