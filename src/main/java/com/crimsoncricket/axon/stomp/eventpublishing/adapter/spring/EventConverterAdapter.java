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
 *
 */

package com.crimsoncricket.axon.stomp.eventpublishing.adapter.spring;

import com.crimsoncricket.axon.stomp.eventpublishing.EventConverter;
import com.crimsoncricket.axon.stomp.eventpublishing.TypedEventConverter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class EventConverterAdapter implements EventConverter, BeanFactoryAware {

	private ConfigurableListableBeanFactory beanFactory;
	private Map<Class, TypedEventConverter> typedConverters = new HashMap<>();

	@SuppressWarnings("unchecked")
	@Override
	public Object convertedEvent(Object anEvent) {
		Optional<TypedEventConverter> typedConverter = typedConverter(anEvent.getClass());
		return typedConverter.map(converter -> converter.converted(anEvent)).orElse(anEvent);
	}

	private Optional<TypedEventConverter> typedConverter(Class eventClass) {
		return Optional.ofNullable(typedConverters.get(eventClass));
	}


	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
		buildConvertersMap();
	}

	private void buildConvertersMap() {
		beanFactory.getBeansWithAnnotation(Converted.class).values()
				.forEach(bean -> {
					ResolvableType beanType = ResolvableType.forClass(bean.getClass());
					Optional<ResolvableType> converterInterface = Arrays.stream(beanType.getInterfaces())
							.filter(iface -> iface.resolve().isAssignableFrom(TypedEventConverter.class))
							.findAny();

					if (converterInterface.isPresent()) {
						ResolvableType genericType = converterInterface.get().getGeneric(0);
						if (genericType.equals(ResolvableType.NONE))
							throw new RuntimeException("TypedEventConverter must be generified with an event type");

						typedConverters.put(genericType.getRawClass(), (TypedEventConverter) bean);
					} else {
						throw new RuntimeException(
								"Beans annotated with @Converted must implement TypedEventConverter"
						);
					}
				});
	}

}
