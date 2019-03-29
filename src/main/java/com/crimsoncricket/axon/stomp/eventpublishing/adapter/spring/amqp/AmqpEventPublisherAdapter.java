/*
 * Copyright 2019 Martijn van der Woud - The Crimson Cricket Internet Services
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

package com.crimsoncricket.axon.stomp.eventpublishing.adapter.spring.amqp;

import com.crimsoncricket.axon.stomp.eventpublishing.AbstractTopicEventPublisher;
import com.crimsoncricket.axon.stomp.eventpublishing.EventConverter;
import com.crimsoncricket.axon.stomp.eventpublishing.EventSerializer;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.stereotype.Component;

@Component
public class AmqpEventPublisherAdapter extends AbstractTopicEventPublisher {

	private final AmqpTemplate amqpTemplate;

	private final AmqpPublisherSettings publisherSettings;

	public AmqpEventPublisherAdapter(
			EventConverter eventConverter,
			EventSerializer eventSerializer,
			AmqpTemplate amqpTemplate,
			AmqpPublisherSettings publisherSettings
	) {
		super(eventConverter, eventSerializer);
		this.amqpTemplate = amqpTemplate;
		this.publisherSettings = publisherSettings;
	}

	@Override
	protected void dispatch(Object serializedEvent, String topic, Class eventClass) {
		if (!(serializedEvent instanceof String))
			throw new RuntimeException("Cannot publish serialized events of type " + serializedEvent.getClass());

		Message message = MessageBuilder
				.withBody(((String) serializedEvent).getBytes())
				.setContentType(publisherSettings.messageContentType())
				.setDeliveryMode(MessageDeliveryMode.PERSISTENT)
				.build();

		amqpTemplate.send(topic, eventClass.getName(), message);
	}
}
