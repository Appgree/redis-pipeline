package com.appgree.core.redis.domain;

/**
 * The callback interface.
 *
 * @param <T> Input parameter
 */
public interface RedisCallback<T> {

    /**
     * Execute.
     *
     * @param value the value
     */
    public void execute(T value);
}
