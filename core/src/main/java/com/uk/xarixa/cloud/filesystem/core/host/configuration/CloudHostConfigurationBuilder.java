package com.uk.xarixa.cloud.filesystem.core.host.configuration;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.host.CloudHostConfigurationType;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;

/**
 * A builder for {@link CloudHostConfiguration} types
 */
public class CloudHostConfigurationBuilder {
	private final static Logger LOG = LoggerFactory.getLogger(CloudHostConfigurationBuilder.class);
	private String settingsName;
	private Class<? extends CloudHostConfiguration> cloudHostConfClass;
	private Map<String,Object> attributes = new HashMap<>();
	
	/**
	 * Uses {@link FastClasspathScanner} to scan all classes for the {@link CloudHostConfigurationType} annotations
	 * @return
	 */
	public static List<String> getAllCloudHostSettingTypes() {
		List<String> settingNames = new ArrayList<>();
		List<ClassInfo> cloudHostSettingsClasses =
				new ClassGraph().enableClassInfo().enableAnnotationInfo()
					.scan().getClassesWithAnnotation(CloudHostConfigurationType.class.getName());
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		for (ClassInfo cloudHostSettingsClassInfo : cloudHostSettingsClasses) {
			String cloudHostSettingsClassName = cloudHostSettingsClassInfo.getName();

			try {
				Class<? extends CloudHostConfiguration> cloudHostSettingsClass =
						(Class<? extends CloudHostConfiguration>)classLoader.loadClass(cloudHostSettingsClassName);
				String name = getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(cloudHostSettingsClass);
				if (StringUtils.isNotBlank(name)) {
					settingNames.add(name);
				}
			} catch (ClassNotFoundException e) {
				LOG.warn("Could not load settings class {} in the classpath", cloudHostSettingsClassName, e);
			} catch (IllegalArgumentException e) {
				LOG.warn("Settings class {} is not valid", cloudHostSettingsClassName, e);
			}
		}

		return settingNames;
	}

	/**
	 * Gets the cloud host configuration class from the name specified in the {@link CloudHostConfiguration} annotations
	 * @param cloudHostConfigurationType
	 * @return
	 */
	public static Class<? extends CloudHostConfiguration> getCloudHostConfigurationClass(String cloudHostConfigurationType) {
		List<ClassInfo> cloudHostSettingsClasses =
				new ClassGraph().enableClassInfo().enableAnnotationInfo()
					.scan().getClassesWithAnnotation(CloudHostConfigurationType.class.getName());
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		for (ClassInfo cloudHostSettingsClassInfo : cloudHostSettingsClasses) {
			String cloudHostSettingsClassName = cloudHostSettingsClassInfo.getName();

			try {
				Class<? extends CloudHostConfiguration> cloudHostConfClass =
						(Class<? extends CloudHostConfiguration>)classLoader.loadClass(cloudHostSettingsClassName);
				String foundName = getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(cloudHostConfClass);
				if (StringUtils.equals(cloudHostConfigurationType, foundName)) {
					return cloudHostConfClass;
				}
			} catch (ClassNotFoundException e) {
				LOG.warn("Could not load cloud host configuration class {} in the classpath", cloudHostSettingsClassName, e);
			} catch (IllegalArgumentException e) {
				LOG.warn("Cloud host configuration class {} is not valid", cloudHostSettingsClassName, e);
			}
		}
		
		return null;
	}

	/**
	 * Gets the name for the cloud host configuration of this class from the {@link CloudHostConfiguration} annotation on the class
	 * @return
	 */
	public static String getCloudHostConfigurationNameFromCloudHostConfigurationClassAnnotation(Class<?> clazz) {
		CloudHostConfigurationType annotation = clazz.getAnnotation(CloudHostConfigurationType.class);
		if (annotation == null) {
			LOG.warn("Cloud host settings class " + clazz +
					" must have a " + CloudHostConfiguration.class.getName() + " annotation");
			return null;
		}
		
		String settingsName = annotation.value();
		if (StringUtils.isBlank(settingsName)) {
			LOG.warn("Cloud host settings class " + clazz +
					" does not have a valid " + CloudHostConfiguration.class.getName() + " annotation, the name '" +
					settingsName + "' is empty");
			return null;
		}

		return settingsName;
	}
	
	/**
	 * Sets the type from a {@link CloudHostConfigurationType} annotation
	 * @param clousHostSettingsTypeName
	 * @return
	 */
	public CloudHostConfigurationBuilder setType(String cloudHostTypeName) {
		Class<? extends CloudHostConfiguration> cloudHostConfClass = getCloudHostConfigurationClass(cloudHostTypeName);
		
		if (cloudHostConfClass == null) {
			throw new IllegalArgumentException("Cannot find a cloud host configuration class with the " +
					CloudHostConfigurationType.class + " annotation set to '" + cloudHostTypeName + "'");
		}
		
		this.cloudHostConfClass = cloudHostConfClass;
		return this;
	}

	public CloudHostConfigurationBuilder setType(Class<? extends CloudHostConfiguration> cloudHostSettingsClass) {
		this.cloudHostConfClass = cloudHostSettingsClass;
		return this;
	}

	/**
	 * Sets the name for this config. This matches the annotation {@link CloudHostConfiguration#getName()}
	 * for a particular class.
	 * @param settingsName
	 */
	public CloudHostConfigurationBuilder setName(String settingsName) {
		if (StringUtils.isBlank(settingsName)) {
			throw new IllegalArgumentException("The cloud host settings name cannot be empty or null, it was '" + settingsName + "'");
		}

		this.settingsName = settingsName;
		return this;
	}

	/**
	 * Sets an attribute to this value on the {@link CloudHostConfiguration} class. This is the bean
	 * property name.
	 * @param attributeName
	 * @param value
	 */
	public CloudHostConfigurationBuilder setAttribute(String attributeName, Object value) {
		// Normalise the name, add the value
		attributeName = Introspector.decapitalize(attributeName);
		Object removed = attributes.put(attributeName, value);
		
		if (removed != null && removed != value) {
			LOG.warn("Attribute '{}' updated or set more than once to a different value", attributeName);
		}

		return this;
	}

	/**
	 * Adds all of the attributes
	 * @see	#setAttribute(String, Object)
	 * @param env
	 * @return
	 */
	public CloudHostConfigurationBuilder setAttributes(Map<String, ?> attributesMap) {
		for (Map.Entry<String, ?> entry : attributesMap.entrySet()) {
			setAttribute(entry.getKey(), entry.getValue());
		}

		return this;
	}
	
	public CloudHostConfigurationBuilder removeAttribute(String attributeName) {
		attributes.remove(attributeName);
		return this;
	}

	protected void validate() {
		if (cloudHostConfClass == null) {
			throw new IllegalStateException("A cloud host config type must be set with the setType method");
		}

		if (settingsName == null) {
			throw new IllegalStateException("A cloud host config name must be set with the setName method");
		}
	}

	/**
	 * Build the {@link CloudHostConfiguration} class
	 * @return
	 * @throws RuntimeException	If an error occurs building the {@link CloudHostConfiguration} class instance
	 */
	public CloudHostConfiguration build() throws RuntimeException {
		validate();
		
		CloudHostConfiguration cloudHostConf;
		try {
			cloudHostConf = cloudHostConfClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Couldn't create an instance of '" + cloudHostConfClass.getName() + "'", e);
		}

		Map<String, PropertyDescriptor> beanProperties = getBeanProperties(cloudHostConfClass);
		cloudHostConf.setName(settingsName);
		
		for (Map.Entry<String, Object> entry : attributes.entrySet()) {
			String propName = entry.getKey();
			PropertyDescriptor propDesc = beanProperties.get(propName);

			if (propDesc == null) {
				throw new RuntimeException("Cannot set unknown property '" + propName + "' for bean " +
						cloudHostConfClass + ", all bean properties for this class: " +
						beanProperties.keySet().toString());
			}

			// Set the attribute
			try {
				propDesc.getWriteMethod().invoke(cloudHostConf, entry.getValue());
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("Error setting property '" + propName + "' for bean " +
						cloudHostConfClass, e);
			}
		}
		
		return cloudHostConf;
	}
	
	protected Map<String,PropertyDescriptor> getBeanProperties(Class<? extends CloudHostConfiguration> cloudHostSettingsClass) {
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(cloudHostSettingsClass);
		} catch (IntrospectionException e) {
			throw new RuntimeException("Error reading bean information for class '" + cloudHostSettingsClass + "'", e);
		}

		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		Map<String,PropertyDescriptor> propDescMap = new HashMap<>();
		for (PropertyDescriptor propDesc : propertyDescriptors) {
			
			// Only include props we can set
			if (propDesc.getWriteMethod() != null) {
				propDescMap.put(propDesc.getName(), propDesc);
			}
		}
		
		return propDescMap;
	}

}
