package com.chiwanpark.flume.plugins;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.nio.charset.Charset;

public class RedisSubscribeDrivenSource extends AbstractSource implements Configurable, EventDrivenSource {

    private Logger logger = LoggerFactory.getLogger(RedisSubscribeDrivenSource.class);

    private ChannelProcessor channelProcessor;

    private Jedis jedis;
    private String redisHost;
    private int redisPort;
    private String redisChannel;
    private int redisTimeout;
    private String messageCharset;

    private boolean runFlag;


    @Override
    public void configure(Context context) {
        redisHost = context.getString("redisHost", "localhost");
        redisPort = context.getInteger("redisPort", 6379);
        redisChannel = context.getString("redisChannel");
        redisTimeout = context.getInteger("redisTimeout", 2000);
        messageCharset = context.getString("messageCharset", "utf-8");

        if (redisChannel == null)
            throw new RuntimeException("Redis Channel must be set.");

        logger.info("Flume Redis Subscribe Source Configured");
    }

    @Override
    public synchronized void start() {
        super.start();

        channelProcessor = getChannelProcessor();

        // TODO: consider using Connection Pool.
        jedis = new Jedis(redisHost, redisPort, redisTimeout);
        logger.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort) + ", timeout: " + String.valueOf(redisTimeout) + ")");

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
            logger.info("Subscribe Manager Thread is started.");

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
            jedisPubSub.unsubscribe(redisChannel);
        }
    }

    private class SubscribeRunner implements Runnable {
        private JedisPubSub jedisPubSub;

        public SubscribeRunner(JedisPubSub jedisPubSub) {
            logger.info("Subscribe Runner Thread is started.");

            this.jedisPubSub = jedisPubSub;
        }

        @Override
        public void run() {
            jedis.subscribe(jedisPubSub, redisChannel);
        }
    }

    private class JedisSubscribeListener extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            Event event = EventBuilder.withBody(message, Charset.forName(messageCharset));
            channelProcessor.processEvent(event);
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
            // TODO: Pattern subscribe feature will be implemented.
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            logger.info("onSubscribe (Channel: " + channel + ")");
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            logger.info("onUnsubscribe (Channel: " + channel + ")");
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
