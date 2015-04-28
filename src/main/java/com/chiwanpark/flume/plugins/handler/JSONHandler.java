/*
 *  Copyright 2015 Chiwan Park
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.chiwanpark.flume.plugins.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;

/**
 * JSONHandler for RedisSource that accepts a string message.
 * <p>
 * This handler throws exception if the deserialization fails because of bad
 * format or any other reason.
 * <p>
 * Each event must be encoded as a map with two key-value pairs. <p> 1. headers
 * - the key for this key-value pair is "headers". The value for this key is
 * another map, which represent the event headers. These headers are inserted
 * into the Flume event as is. <p> 2. body - The body is a string which
 * represents the body of the event. The key for this key-value pair is "body".
 * All key-value pairs are considered to be headers. An example: <p> [{"headers"
 * : {"a":"b", "c":"d"},"body": "random_body"}, {"headers" : {"e": "f"},"body":
 * "random_body2"}] <p> would be interpreted as the following two flume events:
 * <p> * Event with body: "random_body" (in UTF-8/UTF-16/UTF-32 encoded bytes)
 * and headers : (a:b, c:d) <p> *
 * Event with body: "random_body2" (in UTF-8/UTF-16/UTF-32 encoded bytes) and
 * headers : (e:f) <p>
 * <p>
 * The charset of the body is read from the request and used. If no charset is
 * set in the request, then the charset is assumed to be JSON's default - UTF-8.
 * The JSON handler supports UTF-8, UTF-16 and UTF-32.
 */
public class JSONHandler extends RedisSourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JSONHandler.class);
  private final JsonParser parser;

  /**
   * {@inheritDoc}
   */
  public JSONHandler(String charset) {
    super(charset);

    //UTF-8 is default for JSON. If no charset is specified, UTF-8 is to be assumed.
    if (charset == null) {
      LOG.debug("Charset is null, default charset of UTF-8 will be used.");
      this.charset = "UTF-8";
    } else if (!(charset.equalsIgnoreCase("utf-8") || charset.equalsIgnoreCase("utf-16")
        || charset.equalsIgnoreCase("utf-32"))) {
      LOG.error("Unsupported character set in request {}. JSON handler supports UTF-8, "
          + "UTF-16 and UTF-32 only.", charset);
      throw new UnsupportedCharsetException("JSON handler supports UTF-8, UTF-16 and UTF-32 only.");
    }

    parser = new JsonParser();
  }

  public JSONHandler() {
    this(null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Event getEvent(String message) throws Exception {
    /*
     * Gson throws Exception if the data is not parseable to JSON.
     * Need not catch it since the source will catch it and return error.
     */
    JsonObject json = parser.parse(message).getAsJsonObject();

    String body = "";
    JsonElement bodyElm = json.get("body");
    if (bodyElm != null) {
      body = bodyElm.getAsString();
    }

    HashMap<String, String> headers = null;
    if (json.has("headers")) {
      headers = new HashMap<String, String>();
      for (Map.Entry<String, JsonElement> header : json.get("headers").getAsJsonObject().entrySet()) {
        if (header.getValue().isJsonPrimitive()) {
          headers.put(header.getKey(), header.getValue().getAsString());
        } else {
          // If a header is not a json primitive (it contains subfields),
          // we have to use toString() to get it as valid json.
          headers.put(header.getKey(), header.getValue().toString());
        }
      }
    }

    return EventBuilder.withBody(body.getBytes(charset), headers);
  }
}
