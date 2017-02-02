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

import static org.junit.Assert.assertArrayEquals;

@RunWith(JUnit4.class)
public class RawHandlerTest {

  @Test
  public void testRawMessageWithUTF8() throws Exception {
    final String charset = "UTF-8";
    final byte[] testMessage = "test-UTF8, 한글".getBytes();
    RawHandler handler = new RawHandler(charset);
    Event event = handler.getEvent(testMessage);

    assertArrayEquals(testMessage, event.getBody());
  }

  @Test
  public void testSerializationWithUTF8() throws Exception {
    final String charset = "UTF-8";
    final byte[] testMessage = "test-UTF8, 한글".getBytes();
    RawHandler handler = new RawHandler(charset);
    Event event = EventBuilder.withBody(testMessage);

    byte[] result = handler.getBytes(event);
    assertArrayEquals(testMessage, result);
  }
}
