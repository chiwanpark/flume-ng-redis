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

import com.chiwanpark.flume.plugins.handler.RedisSourceHandler;
import com.google.common.base.Throwables;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisSubscribeDrivenSource extends AbstractSource implements Configurable, EventDrivenSource {

  private static final Logger LOG = LoggerFactory.getLogger(RedisSubscribeDrivenSource.class);

  private ChannelProcessor channelProcessor;

  private Jedis jedis;
  private String redisHost;
  private int redisPort;
  private String[] redisChannels;
  private int redisTimeout;
  private String redisPassword;
  private RedisSourceHandler handler;

  private boolean runFlag;


  @Override
  public void configure(Context context) {
    redisHost = context.getString("redisHost", "localhost");
    redisPort = context.getInteger("redisPort", 6379);
    redisTimeout = context.getInteger("redisTimeout", 2000);
    redisPassword = context.getString("redisPassword", "");

    try {
      String charset = context.getString("messageCharset", "utf-8");
      String handlerClassName = context.getString("handler", "com.chiwanpark.flume.plugins.handler.RawHandler");
      @SuppressWarnings("unchecked")
      Class<? extends RedisSourceHandler> clazz = (Class<? extends RedisSourceHandler>) Class.forName(handlerClassName);
      Class[] argTypes = new Class[1];
      argTypes[0] = String.class;
      handler = clazz.getDeclaredConstructor(argTypes).newInstance(charset);
    } catch (ClassNotFoundException ex) {
      LOG.error("Error while configuring RedisSourceHandler. Exception follows.", ex);
      Throwables.propagate(ex);
    } catch (ClassCastException ex) {
      LOG.error("Handler is not an instance of RedisSourceHandler. Handler must implement RedisSourceHandler.");
      Throwables.propagate(ex);
    } catch (Exception ex) {
      LOG.error("Error configuring RedisSubscribeDrivenSource!", ex);
      Throwables.propagate(ex);
    }

    String redisChannel = context.getString("redisChannel");
    if (redisChannel == null) {
      throw new RuntimeException("Redis Channel must be set.");
    }
    redisChannels = redisChannel.split(",");

    LOG.info("Flume Redis Subscribe Source Configured");
  }

  private void connect() {
    LOG.info("Connecting...");
    while (true) {
      try {
        jedis = new Jedis(redisHost, redisPort, redisTimeout);
        if (!"".equals(redisPassword)) {
          jedis.auth(redisPassword);
        } else {
          // Force a connection.
          jedis.ping();
        }
        break;
      } catch (JedisConnectionException e) {
        LOG.error("Connection failed.", e);
        LOG.info("Waiting for 10 seconds...");
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e2) {
          // ?
        }
      }
    }
    LOG.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort)
        + ", timeout: " + String.valueOf(redisTimeout) + ")");
  }

  @Override
  public synchronized void start() {
    super.start();

    channelProcessor = getChannelProcessor();

    // TODO: consider using Connection Pool.
    connect();

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

    // TODO: if we use jedis connection pool, destroy code must be inserted.
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
