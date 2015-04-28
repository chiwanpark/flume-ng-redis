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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.UnsupportedCharsetException;

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
}
