package com.chiwanpark.flume.plugins;

public interface RedisSourceCounterMBean {
    public long getSourceSent();
    public long getSourceRetry();
    public long getSourceError();
}
