package com.appgree.core.redis.provider;

import redis.clients.jedis.JedisPool;

/**
 * The Interface RedisSentinelNotifiable.
 */
public interface RedisSentinelNotifiable {

    /**
     * Pool changed.
     *
     * @param pool the pool
     */
    public void poolChanged(JedisPool pool);

}
