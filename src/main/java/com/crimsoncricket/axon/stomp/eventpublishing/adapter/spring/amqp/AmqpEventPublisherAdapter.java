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
		if (! (serializedEvent instanceof String))
			throw new RuntimeException("Cannot publish serialized events of type " + serializedEvent.getClass());

		Message message = MessageBuilder
				.withBody(((String) serializedEvent).getBytes())
				.setContentType(publisherSettings.messageContentType())
				.setDeliveryMode(MessageDeliveryMode.PERSISTENT)
				.build();

		amqpTemplate.send(topic, eventClass.getName(), message);
	}
}
