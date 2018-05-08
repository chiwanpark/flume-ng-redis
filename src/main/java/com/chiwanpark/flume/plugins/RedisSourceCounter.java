package com.chiwanpark.flume.plugins;

import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.instrumentation.SourceCounterMBean;

public class RedisSourceCounter extends SourceCounter implements SourceCounterMBean {
    private static final String COUNTER_REDIS_SOURCE_SENT = "redis.source.sent";
    private static final String COUNTER_REDIS_SOURCE_RETRY = "redis.source.retry";
    private static final String COUNTER_REDIS_SOURCE_ERROR = "redis.source.error";
    private static final String[] ATTRIBUTES = new String[] {
        COUNTER_REDIS_SOURCE_SENT,
        COUNTER_REDIS_SOURCE_RETRY,
        COUNTER_REDIS_SOURCE_ERROR
    };

    public RedisSourceCounter(String name) {
        super(name, ATTRIBUTES);
    }

    public void incrementSourceSent() {
        this.increment(COUNTER_REDIS_SOURCE_SENT);
    }

    public long getSourceSent() {
        return this.get(COUNTER_REDIS_SOURCE_SENT);
    }

    public void incrementSourceRetry() {
        this.increment(COUNTER_REDIS_SOURCE_RETRY);
    }

    public long getSourceRetry() {
        return this.get(COUNTER_REDIS_SOURCE_RETRY);
    }

    public void incrementSourceError() {
        this.increment(COUNTER_REDIS_SOURCE_ERROR);
    }

    public long getSourceError() {
        return this.get(COUNTER_REDIS_SOURCE_ERROR);
    }
}
