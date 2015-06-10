/*
 *  Copyright 2013 Chiwan Park
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class RedisSubscribeDrivenSourceTest extends RedisSourceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RedisSubscribeDrivenSource.class);

  @Before
  public void setUp() throws Exception {
    context.put("redisChannel", "flume-ng-redis-test");
    source = new RedisSubscribeDrivenSource();

    super.setUp();
  }

  @Test
  public void testSubscribe() throws Exception {
    String message = "testSubscribeMessage!";

    LOG.info("Try to send message to redis source.");
    publishMessageToRedis("flume-ng-redis-test", message);
    Thread.sleep(2000);

    Assert.assertEquals(message, getMessageFromChannel());
  }
}
