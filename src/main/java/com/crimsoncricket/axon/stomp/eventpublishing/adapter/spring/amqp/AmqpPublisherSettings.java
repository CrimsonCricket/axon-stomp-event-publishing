package com.crimsoncricket.axon.stomp.eventpublishing.adapter.spring.amqp;


public class AmqpPublisherSettings {

	private final String messageContentType;


	public AmqpPublisherSettings() {
		this("application/json");
	}

	public AmqpPublisherSettings(String messageContentType) {
		this.messageContentType = messageContentType;
	}

	public String messageContentType() {
		return messageContentType;
	}
}
