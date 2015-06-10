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

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class JSONHandlerTest {
  @Test
  public void testJson() throws Exception {
    String jsonified = "{\"body\": \"hello\"}";
    JSONHandler handler = new JSONHandler("utf-8");

    Event event = handler.getEvent(jsonified);
    assertArrayEquals("hello".getBytes("utf-8"), event.getBody());
  }

  @Test(expected = UnsupportedCharsetException.class)
  public void testJsonWithInvalidEncoding() throws Exception {
    new JSONHandler("utf-12");
  }

  @Test
  public void testJsonWithHeadersAndBody() throws Exception {
    String jsonified = "{\"body\": \"hello\", \"headers\": {\"a\": \"123\", \"b\": \"hello\"}}";
    JSONHandler handler = new JSONHandler("utf-8");

    Event event = handler.getEvent(jsonified);

    assertEquals("123", event.getHeaders().get("a"));
    assertEquals("hello", event.getHeaders().get("b"));
    assertArrayEquals("hello".getBytes("utf-8"), event.getBody());
  }

  @Test
  public void testJsonWithoutEncoding() throws Exception {
    String jsonified = "{\"body\": \"한글\"}";
    JSONHandler handler = new JSONHandler();

    Event event = handler.getEvent(jsonified);

    assertArrayEquals("한글".getBytes("utf-8"), event.getBody());
  }

  @Test
  public void testJsonSerialization() throws Exception {
    final String message = "hello";
    final String charset = "utf-8";
    final String expected = "{\"body\":\"hello\"}";
    JSONHandler handler = new JSONHandler(charset);
    Event event = EventBuilder.withBody(message, Charset.forName(charset));

    String jsonified = handler.getString(event);
    assertEquals(expected, jsonified);
  }

  @Test
  public void testJsonSerializationWithHeadersAndBody() throws Exception {
    final String message = "hello";
    final Map<String, String> headers = new HashMap<String, String>();
    headers.put("hello", "goodbye");

    final String charset = "utf-8";
    final String expected = "{\"body\":\"hello\",\"headers\":{\"hello\":\"goodbye\"}}";
    JSONHandler handler = new JSONHandler(charset);
    Event event = EventBuilder.withBody(message, Charset.forName(charset), headers);

    String jsonified = handler.getString(event);
    assertEquals(expected, jsonified);
  }

  @Test
  public void testJsonSerde() throws Exception {
    final String expected = "{\"body\":\"hello\",\"headers\":{\"hello\":\"goodbye\"}}";
    final String charset = "utf-8";

    JSONHandler handler = new JSONHandler(charset);
    Event event = handler.getEvent(expected);
    String result = handler.getString(event);

    assertEquals(result, expected);
  }
}
