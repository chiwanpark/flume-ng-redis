package com.chiwanpark.flume.plugins;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public abstract class AbstractRedisSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSink.class);

  protected Jedis jedis;

  private String redisHost;
  private int redisPort;
  private int redisTimeout;
  private String redisPassword;
  protected String messageCharset;

  @Override
  public synchronized void start() {
    jedis = new Jedis(redisHost, redisPort, redisTimeout);
    if (!"".equals(redisPassword)) {
      jedis.auth(redisPassword);
    }

    super.start();

    LOG.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort)
        + ", timeout: " + String.valueOf(redisTimeout) + ")");
  }

  @Override
  public synchronized void stop() {
    jedis.disconnect();
    super.stop();
  }

  @Override
  public void configure(Context context) {
    redisHost = context.getString("redisHost", "localhost");
    redisPort = context.getInteger("redisPort", 6379);
    redisTimeout = context.getInteger("redisTimeout", 2000);
    redisPassword = context.getString("redisPassword", "");
    messageCharset = context.getString("messageCharset", "utf-8");
  }
}
