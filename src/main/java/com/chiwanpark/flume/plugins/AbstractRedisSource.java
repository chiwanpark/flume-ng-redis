package com.chiwanpark.flume.plugins;

import com.chiwanpark.flume.plugins.handler.RedisSourceHandler;
import com.google.common.base.Throwables;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class AbstractRedisSource extends AbstractSource implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSource.class);

  protected Jedis jedis;
  protected ChannelProcessor channelProcessor;

  private String redisHost;
  private int redisPort;
  private int redisTimeout;
  private String redisPassword;
  protected RedisSourceHandler handler;

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
      handler = clazz.getDeclaredConstructor(String.class).newInstance(charset);
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
  }

  protected void connect() {
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

    connect();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }
}
