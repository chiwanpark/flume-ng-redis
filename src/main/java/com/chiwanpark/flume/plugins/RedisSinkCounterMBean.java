package com.chiwanpark.flume.plugins;

public interface RedisSinkCounterMBean {
    public long getSinkSendTimeMicros();
    public long getSinkRollback();
    public long getSinkSuccess();
}
