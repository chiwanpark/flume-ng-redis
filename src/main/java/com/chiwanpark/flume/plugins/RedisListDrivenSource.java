package com.chiwanpark.flume.plugins;

import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisListDrivenSource extends AbstractRedisSource implements PollableSource {
  private static final Logger LOG = LoggerFactory.getLogger(RedisListDrivenSource.class);

  private int redisDatabase;
  private String redisList;

  @Override
  public void configure(Context context) {
    redisDatabase = context.getInteger("redisDatabase", 0);
    redisList = context.getString("redisList");
    Preconditions.checkNotNull(redisList, "Redis List must be set.");

    super.configure(context);
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
    String serialized = jedis.rpop(redisList);
    if (serialized == null) {
      return Status.BACKOFF;
    }

    try {
      Event event = messageHandler.getEvent(serialized);
      getChannelProcessor().processEvent(event);
    } catch (ChannelException e) {
      jedis.rpush(redisList, serialized);
      LOG.error("ChannelException is thrown.", e);
    } catch (Exception e) {
      LOG.error("RedisMessageHandler threw unexpected exception.", e);
    }

    return Status.READY;
  }
}
