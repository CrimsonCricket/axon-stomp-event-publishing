/*
 * Copyright 2017 Martijn van der Woud - The Crimson Cricket Internet Services
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.crimsoncricket.axon.stomp.eventpublishing.adapter.spring.beans;

import com.crimsoncricket.axon.stomp.eventpublishing.PublishToTopic;
import com.crimsoncricket.axon.stomp.eventpublishing.PublishToTopics;
import com.crimsoncricket.axon.stomp.eventpublishing.TopicEventPublisher;
import com.crimsoncricket.axon.stomp.eventpublishing.TopicEventPublisherConfigurer;
import org.axonframework.config.EventHandlingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class TopicEventPublisherConfigurerAdapter implements BeanFactoryAware, TopicEventPublisherConfigurer {

	private static final Logger logger = LoggerFactory.getLogger(TopicEventPublisherConfigurerAdapter.class);

	private final TopicEventPublisher topicEventPublisher;
	private ConfigurableListableBeanFactory beanFactory;

	@Autowired
	public TopicEventPublisherConfigurerAdapter(TopicEventPublisher topicEventPublisher) {
		this.topicEventPublisher = topicEventPublisher;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	@Override
	public void registerPublisherInterceptors(EventHandlingConfiguration eventHandlingConfiguration) {
		registerInterceptorsForBeansAnnotatedWithMultipleTopics(eventHandlingConfiguration);
		registerInterceptorsForBeansAnnotatedWithOneTopic(eventHandlingConfiguration);
	}

	private void registerInterceptorsForBeansAnnotatedWithMultipleTopics(
			EventHandlingConfiguration eventHandlingConfiguration
	) {
		String[] annotatedEventHandlerBeans = beanFactory.getBeanNamesForAnnotation(PublishToTopics.class);
		for (String beanName: annotatedEventHandlerBeans) {
			registerMultipleInterceptorsForBean(eventHandlingConfiguration, beanName);
		}
	}

	private void registerMultipleInterceptorsForBean(
			EventHandlingConfiguration eventHandlingConfiguration, String beanName
	) {
		Class beanType = beanFactory.getType(beanName);
		PublishToTopic[] topicAnnotations =
				beanFactory.findAnnotationOnBean(beanName, PublishToTopics.class).value();
		for (PublishToTopic annotation: topicAnnotations)
			registerPublisherInterceptor(eventHandlingConfiguration, beanType, annotation);
	}

	private void registerInterceptorsForBeansAnnotatedWithOneTopic(
			EventHandlingConfiguration eventHandlingConfiguration
	) {
		String[] annotatedEventHandlerBeans = beanFactory.getBeanNamesForAnnotation(PublishToTopic.class);
		for (String beanName: annotatedEventHandlerBeans) {
			Class beanType = beanFactory.getType(beanName);
			PublishToTopic annotation = beanFactory.findAnnotationOnBean(beanName, PublishToTopic.class);
			registerPublisherInterceptor(eventHandlingConfiguration, beanType, annotation);
		}
	}

	private void registerPublisherInterceptor(
			EventHandlingConfiguration eventHandlingConfiguration,
			Class beanType,
			PublishToTopic annotation
	) {
		String processorName = beanType.getPackage().getName();
		String annotatedTopic = annotation.value();
		Class eventClass = annotation.eventClass();
		List<Class> skipClasses = Arrays.asList(annotation.skipClasses());

		eventHandlingConfiguration.registerHandlerInterceptor(
				processorName,
				configuration -> ((unitOfWork, interceptorChain) -> {
					unitOfWork.afterCommit((u) -> {
						Object payload = u.getMessage().getPayload();
						try {
							if (! matchesClass(eventClass, skipClasses, payload))
								return;
							String resolvedTopic = resolvedTopic(annotatedTopic, payload);
							topicEventPublisher.publishEventToTopic(payload, resolvedTopic);
						} catch (Exception e) {
							logger.warn("Error resolving topic " + annotatedTopic, e);
						}
					});
					return interceptorChain.proceed();
				})
		);
	}

	private boolean matchesClass(Class<?> eventClass, List<Class> skipClasses, Object payload) {
		Class payloadClass = payload.getClass();
		for (Class<?> skipClass: skipClasses) {
			if (skipClass.isAssignableFrom(payloadClass)) {
				logger.debug("Payload class " + payloadClass.getName() + " matches skip class " + skipClass.getName());
				return false;
			}
		}
		boolean matches = eventClass.isAssignableFrom(payloadClass);
		logger.debug(
				"Checked if payload class " + payloadClass.getName() + " matches " + eventClass +
						"; result: " + matches
		);
		return matches;
	}


	private String resolvedTopic(String annotatedTopic, Object payload) throws Exception {
		String resolvedTopic = annotatedTopic;

		List<String> placeholders = placeHoldersInAnnotatedTopic(annotatedTopic);
		for (String placeHolder: placeholders) {
			String placeHolderValue = placeHolderValue(placeHolder, payload);
			resolvedTopic = topicWithPlaceHolderValue(resolvedTopic, placeHolder, placeHolderValue);
		}

		return resolvedTopic;
	}

	private List<String> placeHoldersInAnnotatedTopic(String annotatedTopic) {
		Pattern pattern = Pattern.compile("\\{[^\\}]+\\}");  // matches: ... {somePlaceHolder} ....
		Matcher matcher = pattern.matcher(annotatedTopic);
		List<String> placeHolders = new ArrayList<>();
		while (matcher.find())
			placeHolders.add(matcher
					.group()
					.replaceAll("^\\{", "")
					.replaceAll("\\}$", "")
			);
		return placeHolders; // a list of placeholders, without the surrounding curly braces
	}

	private String placeHolderValue(String placeHolder, Object payload) throws Exception {
		String[] placeholderParts = placeHolder.split("\\.");
		Object resolvedValue = payload;
		for (String placeholderPart : placeholderParts) {
			Class<?> valueClass = resolvedValue.getClass();
			Method accessorMethod = valueClass.getMethod(placeholderPart);
			resolvedValue = accessorMethod.invoke(resolvedValue);
			if (resolvedValue == null)
				throw new Exception(
						"Could not resolve placeholder " + placeHolder + "; method " +
								placeholderPart + " returned null"
				);

		}
		return resolvedValue.toString();
	}

	private String topicWithPlaceHolderValue(String topic, String placeHolder, String placeHolderValue) {
		return topic.replace("{" + placeHolder + "}", placeHolderValue);
	}

}
