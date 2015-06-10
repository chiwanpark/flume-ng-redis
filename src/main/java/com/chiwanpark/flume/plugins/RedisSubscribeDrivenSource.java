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
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisSubscribeDrivenSource extends AbstractRedisSource implements EventDrivenSource {

  private static final Logger LOG = LoggerFactory.getLogger(RedisSubscribeDrivenSource.class);

  private String[] redisChannels;
  private boolean runFlag;

  @Override
  public void configure(Context context) {
    String redisChannel = context.getString("redisChannel");
    Preconditions.checkNotNull(redisChannel, "Redis Channel must be set.");
    redisChannels = redisChannel.split(",");

    super.configure(context);
    LOG.info("Flume Redis Subscribe Source Configured");
  }

  @Override
  public synchronized void start() {
    super.start();

    runFlag = true;
    new Thread(new SubscribeManager()).start();
  }

  @Override
  public synchronized void stop() {
    super.stop();

    runFlag = false;

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private class SubscribeManager implements Runnable {

    private Thread subscribeRunner;
    private JedisPubSub jedisPubSub;

    @Override
    public void run() {
      LOG.info("Subscribe Manager Thread is started.");

      jedisPubSub = new JedisSubscribeListener();
      subscribeRunner = new Thread(new SubscribeRunner(jedisPubSub));

      subscribeRunner.start();

      while (runFlag) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // TODO: think better way for thread safety.
      jedisPubSub.unsubscribe(redisChannels);
    }
  }

  private class SubscribeRunner implements Runnable {

    private JedisPubSub jedisPubSub;

    public SubscribeRunner(JedisPubSub jedisPubSub) {
      LOG.info("Subscribe Runner Thread is started.");

      this.jedisPubSub = jedisPubSub;
    }

    @Override
    public void run() {
      while (runFlag) {
        try {
          jedis.subscribe(jedisPubSub, redisChannels);
        } catch (JedisConnectionException e) {
          LOG.error("Disconnected from Redis...");
          connect();
        } catch (Exception e) {
          LOG.error("FATAL ERROR: unexpected exception in SubscribeRunner.", e);
        }
      }
    }
  }

  private class JedisSubscribeListener extends JedisPubSub {

    @Override
    public void onMessage(String channel, String message) {
      try {
        channelProcessor.processEvent(handler.getEvent(message));
      } catch (Exception e) {
        LOG.error("RedisSourceHandler threw unexpected exception.", e);
      }
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
      // TODO: Pattern subscribe feature will be implemented.
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
      LOG.info("onSubscribe (Channel: " + channel + ")");
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
      LOG.info("onUnsubscribe (Channel: " + channel + ")");
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
      // TODO: Pattern subscribe feature will be implemented.
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
      // TODO: Pattern subscribe feature will be implemented.
    }
  }
}
