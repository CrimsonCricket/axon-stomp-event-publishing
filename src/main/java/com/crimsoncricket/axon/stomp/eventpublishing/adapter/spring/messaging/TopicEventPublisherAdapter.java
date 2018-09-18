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

package com.crimsoncricket.axon.stomp.eventpublishing.adapter.spring.messaging;

import com.crimsoncricket.axon.stomp.eventpublishing.AbstractTopicEventPublisher;
import com.crimsoncricket.axon.stomp.eventpublishing.EventConverter;
import com.crimsoncricket.axon.stomp.eventpublishing.EventSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class TopicEventPublisherAdapter extends AbstractTopicEventPublisher {

	private final SimpMessagingTemplate messagingTemplate;

	@Autowired
	public TopicEventPublisherAdapter(
			EventConverter eventConverter,
			EventSerializer eventSerializer,
			SimpMessagingTemplate messagingTemplate) {
		super(eventConverter, eventSerializer);
		this.messagingTemplate = messagingTemplate;
	}

	@Override
	protected void dispatch(Object serializedEvent, String topic, Class eventClass) {
		Map<String, Object> headers = new HashMap<>();
		headers.put("eventType", eventClass.getName());
		messagingTemplate.convertAndSend("/topic" + topic, serializedEvent, headers);
	}

}
