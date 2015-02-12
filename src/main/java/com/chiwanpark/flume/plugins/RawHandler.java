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
package com.chiwanpark.flume.plugins;

import java.nio.charset.Charset;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

/**
 *
 * RawHandler for RedisSource that accepts a string message and converts it
 * to a flume Event having no headers and a body set to message.
 *
 */

public class RawHandler extends RedisSourceHandler {

  /**
   * {@inheritDoc}
   */
  public RawHandler(String charset) {
    super(charset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Event getEvent(String message) throws Exception {
    return EventBuilder.withBody(message.getBytes(charset));
  }
}
