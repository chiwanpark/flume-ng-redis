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

import org.apache.flume.Event;
import java.nio.charset.Charset;

/**
 *
 */
public abstract class RedisSourceHandler {

  protected String charset;

  /**
   * Constructor.
   *
   * @param charset The charset of the messages.
  */
  public RedisSourceHandler(String charset) {
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
}
