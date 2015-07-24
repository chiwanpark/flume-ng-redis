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

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

public class RedisPublishDrivenSink extends AbstractRedisSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(RedisPublishDrivenSink.class);

  private String redisChannel;

  @Override
  public void configure(Context context) {
    redisChannel = context.getString("redisChannel");

    Preconditions.checkNotNull(redisChannel, "Redis Channel must be set.");

    super.configure(context);
    LOG.info("Flume Redis Publish Sink Configured");
  }

  @Override
  public synchronized void stop() {
    jedis.disconnect();
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      Event event = channel.take();
      String serialized = messageHandler.getString(event);

      if (jedis.publish(redisChannel, serialized) > 0) {
        transaction.commit();
        status = Status.READY;
      } else {
        throw new EventDeliveryException(
            "Event is published, but there is no receiver in this channel named " + redisChannel);
      }
    } catch (Throwable e) {
      transaction.rollback();
      status = Status.BACKOFF;

      // we need to rethrow jedis exceptions, because they signal that something went wrong
      // with the connection to the redis server
      if (e instanceof JedisException) {
        throw new EventDeliveryException(e);
      }

      if (e instanceof Error) {
        throw (Error) e;
      }
    } finally {
      transaction.close();
    }

    return status;
  }
}
