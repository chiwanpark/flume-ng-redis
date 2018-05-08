package com.chiwanpark.flume.plugins;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

public class RedisListDrivenSink extends AbstractRedisSink {
  private static final Logger LOG = LoggerFactory.getLogger(RedisListDrivenSink.class);

  private int redisDatabase;
  private byte[] redisList;

  @Override
  public void configure(Context context) {
    redisDatabase = context.getInteger("redisDatabase", 0);
    redisList = context.getString("redisList").getBytes();

    Preconditions.checkNotNull(redisList, "Redis List must be set.");

    super.configure(context);
    LOG.info("Flume Redis List Sink Configured");
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  @Override
  public synchronized void start() {
    super.start();

    if (redisDatabase != 0) {
      final String result = jedis.select(redisDatabase);
      if (!"OK".equals(result)) {
        throw new RuntimeException("Cannot select database (database: " + redisDatabase + ")");
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      long startTime = System.nanoTime();

      Event event = channel.take();
      byte[] serialized = messageHandler.getBytes(event);

      if (jedis.lpush(redisList, serialized) > 0) {
        transaction.commit();
        long endTime = System.nanoTime();
        counter.incrementSinkSendTimeMicros((endTime - startTime) / (1000));
        counter.incrementSinkSuccess();
        status = Status.READY;
      } else {
        throw new EventDeliveryException("Event cannot be pushed into list " + redisList);
      }
    } catch (Throwable e) {
      transaction.rollback();
      counter.incrementSinkRollback();
      status = Status.BACKOFF;

      // we need to rethrow jedis exceptions, because they signal that something went wrong
      // with the connection to the redis server
      if (e instanceof JedisException) {
        // TODO: we could try to reconnect and resend immediately
        jedis.disconnect();
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
