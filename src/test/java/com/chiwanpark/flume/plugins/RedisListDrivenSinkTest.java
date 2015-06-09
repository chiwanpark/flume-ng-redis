package com.chiwanpark.flume.plugins;

import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.AbstractSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.nio.charset.Charset;

@RunWith(JUnit4.class)
public class RedisListDrivenSinkTest {
  private static final String TEST_LIST = "flume-ng-redis-test";
  private static final String TEST_MESSAGE = "flume-ng-redis-test-message";
  private static final Logger LOG = LoggerFactory.getLogger(RedisListDrivenSinkTest.class);

  private Jedis jedis;
  private Context context = new Context();
  private Channel channel = new MemoryChannel();
  private ChannelSelector channelSelector = new ReplicatingChannelSelector();
  private AbstractSink sink;

  @Before
  public void setUp() throws Exception {
    context.put("redisList", TEST_LIST);

    Configurables.configure(channel, context);
    channelSelector.setChannels(Lists.newArrayList(channel));

    sink = new RedisListDrivenSink();
    sink.setChannel(channel);

    jedis = new Jedis("localhost", 6379);
  }

  @After
  public void tearDown() throws Exception {
    jedis.disconnect();
    channel.stop();
    sink.stop();
  }

  @Test
  public void testPushEvent() throws Exception {
    LOG.info("Test Publish feature in RedisListDrivenSink");

    Configurables.configure(sink, context);
    sink.start();
    channel.start();

    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      LOG.info("Put test message into channel");
      channel.put(EventBuilder.withBody(TEST_MESSAGE, Charset.forName("utf-8")));
      transaction.commit();
    } catch (Throwable e) {
      LOG.info("Rollback");
      transaction.rollback();
    } finally {
      LOG.info("Transaction is closed");
      transaction.close();
    }

    Thread.sleep(1000);
    sink.process();

    Thread.sleep(1000);
    Assert.assertEquals(TEST_MESSAGE, jedis.rpop(TEST_LIST));
  }

  @Test
  public void testSelectDatabase() throws Exception {
    context.put("redisDatabase", "1");

    Configurables.configure(sink, context);
    sink.start();
    channel.start();

    Assert.assertEquals("OK", jedis.select(1));

    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      LOG.info("Put test message into channel");
      channel.put(EventBuilder.withBody(TEST_MESSAGE, Charset.forName("utf-8")));
      transaction.commit();
    } catch (Throwable e) {
      LOG.info("Rollback");
      transaction.rollback();
    } finally {
      LOG.info("Transaction is closed");
      transaction.close();
    }

    Thread.sleep(1000);
    sink.process();

    Thread.sleep(1000);
    Assert.assertEquals(TEST_MESSAGE, jedis.rpop(TEST_LIST));
  }
}
