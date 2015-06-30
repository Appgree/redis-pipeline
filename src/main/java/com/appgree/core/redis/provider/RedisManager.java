package com.appgree.core.redis.provider;


/**
 * The Class RedisManager.
 */
public class RedisManager {

    /** The provider. */
    private static RedisProvider provider;

    // Private constructor prevents instantiation from other classes
    /**
     * Instantiates a new redis manager.
     */
    private RedisManager() {}

    // Returns the instance
    /**
     * Gets the single instance of RedisManager.
     *
     * @return single instance of RedisManager
     */
    public static RedisProvider getInstance() {
        return provider;
    }

    /**
     * Static init.
     *
     * @param provider the provider
     */
    public static void staticInit(RedisProvider provider) {
        RedisManager.provider = provider;
    }

    /**
     * Uninit all.
     */
    public static void uninitAll() {
        provider = null;
    }

    /**
     * this is to avoid Thread.Sleep() in integration test, and only for testing purposes
     */
    public static void flushLocalCache() {
        if (provider != null && provider instanceof RedisCachedProvider) {
            ((RedisCachedProvider) provider).flushLocalCache();
        }
    }
}
