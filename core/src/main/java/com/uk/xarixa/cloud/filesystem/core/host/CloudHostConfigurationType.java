package com.uk.xarixa.cloud.filesystem.core.host;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfiguration;
import com.uk.xarixa.cloud.filesystem.core.host.configuration.CloudHostConfigurationBuilder;

/**
 * This annotation should be added to {@link CloudHostConfiguration} classes created through the
 * {@link CloudHostConfigurationBuilder} so that the config class can be retrieved by
 * the type name.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CloudHostConfigurationType {

	String value();

}
