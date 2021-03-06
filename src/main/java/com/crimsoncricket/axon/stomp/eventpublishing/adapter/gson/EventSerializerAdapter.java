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

package com.crimsoncricket.axon.stomp.eventpublishing.adapter.gson;

import com.crimsoncricket.axon.stomp.eventpublishing.EventSerializer;
import com.google.gson.*;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.util.Base64;

@Component
public class EventSerializerAdapter implements EventSerializer {

	private final Gson gson;

	public EventSerializerAdapter() {
		gson = new GsonBuilder()
				.registerTypeAdapter(byte[].class, new ByteArrayToBase64TypeAdapter())
				.create();
	}

	@Override
	public String serialize(Object anEvent) {
		return gson.toJson(anEvent);
	}

	private static class ByteArrayToBase64TypeAdapter implements JsonSerializer<byte[]> {

		@Override
		public JsonElement serialize(byte[] bytes, Type type, JsonSerializationContext jsonSerializationContext) {
			return new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
		}
	}

}
