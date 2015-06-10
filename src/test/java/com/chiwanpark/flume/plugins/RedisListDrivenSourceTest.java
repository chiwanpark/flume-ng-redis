package com.chiwanpark.flume.plugins;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class RedisListDrivenSourceTest extends RedisSourceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RedisListDrivenSourceTest.class);

  @Before
  public void setUp() throws Exception {
    context.put("redisList", "flume-ng-redis-test");
    source = new RedisListDrivenSource();

    super.setUp();
  }

  @Test
  public void testList() throws Exception {
    String message = "testListMessage";

    LOG.info("Try to send message to redis source.");
    addMessageToRedisList("flume-ng-redis-test", message);
    Thread.sleep(2000);

    ((RedisListDrivenSource) source).process();
    Assert.assertEquals(message, getMessageFromChannel());
  }
}
