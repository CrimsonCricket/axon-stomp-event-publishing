package com.crimsoncricket.axon.stomp.eventpublishing.adapter.spring.kafka;

import com.crimsoncricket.axon.stomp.eventpublishing.AbstractTopicEventPublisher;
import com.crimsoncricket.axon.stomp.eventpublishing.EventConverter;
import com.crimsoncricket.axon.stomp.eventpublishing.EventSerializer;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class KafkaEventPublisherAdapter extends AbstractTopicEventPublisher {

    private final KafkaOperations<String, byte[]> kafkaOperations;

    public KafkaEventPublisherAdapter(
            EventConverter eventConverter,
            EventSerializer eventSerializer,
            KafkaOperations<String, byte[]> kafkaOperations
    ) {
        super(eventConverter, eventSerializer);
        this.kafkaOperations = kafkaOperations;
    }

    @Override
    protected void dispatch(String serializedEvent, String topic, Class<?> eventClass) {
        var messageBuilder = MessageBuilder
                .withPayload(serializedEvent.getBytes(UTF_8))
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(HeaderNames.PAYLOAD_TYPE, eventClass.getName());

        CurrentUnitOfWork.ifStarted(unitOfWork -> {
            var message = unitOfWork.getMessage();
            if (message instanceof EventMessage) {
                messageBuilder.setHeader(KafkaHeaders.TIMESTAMP, ((EventMessage<?>) message).getTimestamp().toEpochMilli());
            }
            if (message instanceof DomainEventMessage) {
                messageBuilder.setHeader(KafkaHeaders.KEY, ((DomainEventMessage<?>) message).getAggregateIdentifier());
            }
        });
        var message = messageBuilder.build();
        kafkaOperations.send(message);
    }
}
