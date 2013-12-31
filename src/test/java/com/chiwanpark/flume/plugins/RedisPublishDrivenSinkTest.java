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

import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

@RunWith(JUnit4.class)
public class RedisPublishDrivenSinkTest {

  private static final String TEST_CHANNEL = "flume-ng-redis-test";

  private Logger logger = LoggerFactory.getLogger(RedisPublishDrivenSinkTest.class);
  private Context context = new Context();
  private Channel channel = new MemoryChannel();
  private ChannelSelector channelSelector = new ReplicatingChannelSelector();
  private AbstractSink sink;
  private TestSubscriptionListener listener;
  private Jedis jedis;
  private Thread thread;

  @Before
  public void setUp() throws Exception {
    context.clear();
    context.put("redisChannel", TEST_CHANNEL);

    Configurables.configure(channel, context);
    channelSelector.setChannels(Lists.newArrayList(channel));

    sink = new RedisPublishDrivenSink();
    sink.setChannel(channel);
    Configurables.configure(sink, context);

    sink.start();
    channel.start();
    Thread.sleep(1000);

    jedis = new Jedis("localhost", 6379);
    listener = new TestSubscriptionListener();

    thread = new Thread(new Runnable() {
      @Override
      public void run() {
        jedis.subscribe(listener, TEST_CHANNEL);
      }
    });

    thread.start();
    Thread.sleep(1000);
  }

  @After
  public void tearDown() throws Exception {
    thread.interrupt();

    channel.stop();
    sink.stop();
    jedis.disconnect();
  }

  @Test
  public void testPublish() throws Exception {
    logger.info("Test Publish feature in RedisPublishDrivenSink");
    String message = "flume-ng-redis-test-message";

    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      logger.info("Put test message into channel");
      channel.put(EventBuilder.withBody(message, Charset.forName("utf-8")));
      transaction.commit();
    } catch (Throwable e) {
      logger.info("Rollback");
      transaction.rollback();
    } finally {
      logger.info("Transaction is closed");
      transaction.close();
    }

    Thread.sleep(1000);
    sink.process();

    Thread.sleep(1000);
    Assert.assertEquals(message, listener.pollMessage());
  }

  private class TestSubscriptionListener extends JedisPubSub {

    private Queue<String> queue = new ConcurrentLinkedQueue<String>();

    public String pollMessage() {
      String message;

      do {
        message = queue.poll();
      } while (message == null);

      return message;
    }

    @Override
    public void onMessage(String channel, String message) {
      logger.info("Message Arrived!: " + message);

      queue.add(message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
      // ignored
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
      logger.info("Redis Channel " + channel + " is subscribed.");
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
      logger.info("Redis Channel " + channel + " is unsubscribed.");
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
      // ignored
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
      // ignored
    }
  }
}
