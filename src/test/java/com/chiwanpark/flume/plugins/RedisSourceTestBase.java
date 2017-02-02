package com.chiwanpark.flume.plugins;

import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

@RunWith(JUnit4.class)
public abstract class RedisSourceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RedisSourceTestBase.class);

  protected Context context = new Context();
  protected Channel channel = new MemoryChannel();
  protected ChannelSelector channelSelector = new ReplicatingChannelSelector();
  protected AbstractSource source;

  @Before
  public void setUp() throws Exception {
    Configurables.configure(channel, context);
    channelSelector.setChannels(Lists.newArrayList(channel));

    source.setChannelProcessor(new ChannelProcessor(channelSelector));
    Configurables.configure(source, context);

    LOG.info("Try to start RedisSubscribeDrivenSource.");
    source.start();
    Thread.sleep(1000);
  }

  @After
  public void tearDown() throws Exception {
    source.stop();
  }

  protected void publishMessageToRedis(String channel, String message) throws Exception {
    Jedis jedis = new Jedis("localhost", 6379);

    jedis.publish(channel, message);

    jedis.disconnect();
  }

  protected void addMessageToRedisList(String list, String message) throws Exception {
    Jedis jedis = new Jedis("localhost", 6379);

    jedis.lpush(list, message);

    jedis.disconnect();
  }

  protected String getMessageFromChannel() throws Exception {
    Transaction transaction = channel.getTransaction();
    try {
      transaction.begin();

      Event event;

      do {
        event = channel.take();
      } while (event == null);
      transaction.commit();

      return new String(event.getBody(), "UTF-8");
    } finally {
      transaction.close();
    }
  }
}
