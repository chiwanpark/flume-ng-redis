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

/**
 * Message handler interface for RedisSource and RedisSink.
 */
public abstract class RedisMessageHandler {

  protected String charset;

  /**
   * Constructor.
   *
   * @param charset The charset of the messages.
   */
  public RedisMessageHandler(String charset) {
    this.charset = charset;
  }

  /**
   * Takes a string message and returns a Flume Event. If this request
   * cannot be parsed into Flume events based on the format this method
   * will throw an exception. This method may also throw an
   * exception if there is some sort of other error. <p>
   *
   * @param message The message to be parsed into Flume events.
   * @return Flume event generated from the request.
   * @throws Exception If there was an unexpected error.
   */
  public abstract Event getEvent(String message) throws Exception;

  /**
   * Takes a event and returns a string representing the event. The result
   * of this method could be parsed as a Flume event by getEvent method. If
   * this request cannot succeed, this method will throw exception.
   *
   * @param event The event to be serialized.
   * @return String representing the given event.
   * @throws Exception If there was an unexpected error.
   */
  public abstract String getString(Event event) throws Exception;
}
