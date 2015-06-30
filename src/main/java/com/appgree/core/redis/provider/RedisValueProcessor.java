package com.appgree.core.redis.provider;

/**
 * The Redis value processor enables extra processing after getting a Redis value. The cached version of the object will store the result value and
 * use it next time saving processing.
 *
 * @param <Z> The result type
 * @param <V> The Redis value type
 */
public interface RedisValueProcessor<Z, V> {

    /**
     * Process a Redis value and returns the generated object This enables cached objects to store a processed object.
     *
     * @param value The Redis value input
     * @return The generated object
     */
    Z process(RedisValue<V> value);

}
