package com.appgree.core.redis.provider;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * The Class RedisRemoteBaseProvider.
 */
public abstract class RedisRemoteBaseProvider implements RedisProvider, RedisSentinelNotifiable {

    /** The pool. */
    protected volatile JedisPool pool;

    /** The sentinel master name. */
    protected volatile String sentinelMasterName;

    /**
     * Instantiates a new redis remote base provider.
     *
     * @throws Exception the exception
     */
    public RedisRemoteBaseProvider() throws Exception {}

    /**
     * Open jedis.
     *
     * @return the jedis
     */
    public Jedis openJedis() {
        return pool.getResource();
    }

    /**
     * Close jedis.
     *
     * @param jedis the jedis
     */
    public void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisSentinelNotifiable#poolChanged(redis.clients.jedis.JedisPool)
     */
    @Override
    public void poolChanged(JedisPool pool) {
        this.pool = pool;
    }
}
