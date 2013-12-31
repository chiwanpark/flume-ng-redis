package com.chiwanpark.flume.plugins;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class RedisPublishDrivenSink extends AbstractSink implements Configurable {

  private Logger logger = LoggerFactory.getLogger(RedisPublishDrivenSink.class);

  private Jedis jedis;

  private String redisHost;
  private int redisPort;
  private String redisChannel;
  private int redisTimeout;
  private String messageCharset;

  @Override
  public void configure(Context context) {
    redisHost = context.getString("redisHost", "localhost");
    redisPort = context.getInteger("redisPort", 6379);
    redisChannel = context.getString("redisChannel");
    redisTimeout = context.getInteger("redisTimeout", 2000);
    messageCharset = context.getString("messageCharset", "utf-8");

    if (redisChannel == null) { throw new RuntimeException("Redis Channel must be set."); }

    logger.info("Flume Redis Publish Sink Configured");
  }

  @Override
  public synchronized void start() {
    jedis = new Jedis(redisHost, redisPort, redisTimeout);

    logger.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort)
                + ", timeout: " + String.valueOf(redisTimeout) + ")");
    super.start();
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

      if (jedis.publish(redisChannel, new String(event.getBody(), messageCharset)) > 0) {
        transaction.commit();
        status = Status.READY;
      } else {
        throw new EventDeliveryException(
            "Event is published, but there is no receiver in this channel named " + redisChannel);
      }
    } catch (Throwable e) {
      transaction.rollback();
      status = Status.BACKOFF;

      if (e instanceof Error) {
        throw (Error) e;
      }
    } finally {
      transaction.close();
    }

    return status;
  }
}
