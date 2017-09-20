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

package com.crimsoncricket.axon.stomp.eventpublishing;

public abstract class AbstractTopicEventPublisher implements TopicEventPublisher {

	private final EventConverter eventConverter;
	private final EventSerializer eventSerializer;

	protected AbstractTopicEventPublisher(EventConverter eventConverter, EventSerializer eventSerializer) {
		this.eventConverter = eventConverter;
		this.eventSerializer = eventSerializer;
	}

	@Override
	public void publishEventToTopic(Object anEvent, String topic) {
		Object convertedEvent = eventConverter.convertedEvent(anEvent);
		String serializedEvent = eventSerializer.serialize(convertedEvent);
		dispatch(serializedEvent, topic, convertedEvent.getClass());
	}

	protected abstract void dispatch(Object serializedEvent, String topic, Class eventClass);


}
