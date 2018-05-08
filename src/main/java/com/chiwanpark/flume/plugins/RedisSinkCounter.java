package com.chiwanpark.flume.plugins;

import org.apache.flume.instrumentation.SinkCounter;

public class RedisSinkCounter extends SinkCounter implements RedisSinkCounterMBean {
    private static final String COUNTER_REDIS_SINK_SEND_TIME_MICROS = "redis.sink.sendTimeMicros";
    private static final String COUNTER_REDIS_SINK_ROLLBACK = "redis.sink.rollback";
    private static final String COUNTER_REDIS_SINK_SUCCESS = "redis.sink.success";

    private static final String[] ATTRIBUTES = new String[] {
        COUNTER_REDIS_SINK_SEND_TIME_MICROS,
        COUNTER_REDIS_SINK_ROLLBACK,
        COUNTER_REDIS_SINK_SUCCESS
    };

    public RedisSinkCounter(String name) {
        super(name, ATTRIBUTES);
    }

    public void incrementSinkSendTimeMicros(long delta) {
        this.addAndGet(COUNTER_REDIS_SINK_SEND_TIME_MICROS, delta);
    }

    public long getSinkSendTimeMicros() {
        return this.get(COUNTER_REDIS_SINK_SEND_TIME_MICROS);
    }

    public void incrementSinkRollback() {
        this.increment(COUNTER_REDIS_SINK_ROLLBACK);
    }

    public long getSinkRollback() {
        return this.get(COUNTER_REDIS_SINK_ROLLBACK);
    }

    public void incrementSinkSuccess() {
        this.increment(COUNTER_REDIS_SINK_SUCCESS);
    }

    public long getSinkSuccess() {
        return this.get(COUNTER_REDIS_SINK_SUCCESS);
    }
}
