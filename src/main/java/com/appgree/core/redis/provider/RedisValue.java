package com.appgree.core.redis.provider;

import org.apache.log4j.Logger;

import redis.clients.jedis.Response;

import com.appgree.core.redis.domain.RedisCallback;

/**
 * The delayed response wrapper.
 *
 * @param <T> The return type
 */
public class RedisValue<T> {

    /** The logger. */
    private static Logger logger = Logger.getLogger(RedisValue.class.getName());

    /** The value. */
    private T value;

    /** The must wait. */
    private boolean mustWait = true;

    /** The value set. */
    private boolean valueSet = false;

    /** The response. */
    private Response<T> response;

    /** The callback. */
    private RedisCallback<T> callback;

    /** The pipelined manager. */
    private RedisPipelinedManager pipelinedManager;

    /** The flushed. */
    private boolean flushed = false;

    /** The auto flush. */
    private boolean autoFlush = false;

    /** The processed value. */
    private volatile Object processedValue = null;

    /** The is processed. */
    private volatile boolean isProcessed = false;

    /** The ttl reference. */
    private long ttlReference = 0;

    /**
     * Sets the pipelined manager.
     *
     * @param pipelinedManager the new pipelined manager
     */
    void setPipelinedManager(RedisPipelinedManager pipelinedManager) {
        this.pipelinedManager = pipelinedManager;
    }

    /**
     * Flush.
     *
     * @return the redis value
     */
    synchronized public RedisValue<T> flush() {
        if (this.flushed) {
            return this;
        }

        setFlushed();

        if (this.pipelinedManager != null) {
            this.pipelinedManager.flush();
        }

        return this;
    }

    /**
     * Sets the flushed.
     */
    synchronized void setFlushed() {
        this.flushed = true;
        this.autoFlush = false;
    }

    /**
     * Sets the value.
     *
     * @param value the value
     * @return the redis value
     */
    synchronized RedisValue<T> setValue(T value) {

        this.value = value;

        this.valueSet = true;

        this.ttlReference = System.currentTimeMillis();

        setFlushed();

        executeCallBack();

        // wake it up
        this.mustWait = false;

        notifyAll();

        return this;
    }

    /**
     * Sets the response.
     *
     * @param response the response
     * @return the redis value impl
     */
    RedisValue<T> setResponse(Response<T> response) {
        this.response = response;
        return this;
    }

    /**
     * Gets the response.
     *
     * @return the response
     */
    Response<T> getResponse() {
        return this.response;
    }

    /**
     * Sets the call back.
     *
     * @param callback the new call back
     */
    public synchronized void setCallBack(RedisCallback<T> callback) {

        this.callback = callback;

        executeCallBack();
    }

    /**
     * Gets the.
     *
     * @return the t
     */
    public synchronized T get() {

        if (this.autoFlush && this.flushed == false) {
            flush();
        }

        if (mustWait) {
            RedisPipelinedManager.incrBlockingCounter(1);
            try {

                // waits for notification
                wait(RedisPipelinedManager.getMaxWaitTimeout());

                // this will prevent errors when closing main container
                if (pipelinedManager != null && pipelinedManager.isShutdown()) {
                    throw new RuntimeException("Service is shutting down unexpectedly!");
                }

                // checks timeout state
                if (mustWait) {
                    throw new RuntimeException("Cache: timeout in get() method");
                }

            } catch (InterruptedException e) {
                throw new RuntimeException("Cache: Interrupted", e);
            } finally {
                RedisPipelinedManager.incrBlockingCounter(-1);
            }
        }

        return value;
    }

    /**
     * Execute call back.
     */
    private synchronized void executeCallBack() {
        try {
            if (this.callback != null && valueSet) {
                this.callback.execute(this.value);
            }
        } catch (Throwable e) {
            logger.error("Exception executing callback", e);
        }
    }

    /**
     * Sets the auto flush.
     */
    public void setAutoFlush() {
        this.autoFlush = true;
    }

    /**
     * Gets the.
     *
     * @param <Z> the generic type
     * @param processor the processor
     * @return the z
     */
    @SuppressWarnings("unchecked")
    public <Z> Z get(RedisValueProcessor<Z, T> processor) {

        // checks the most common case
        if (this.isProcessed) {
            return (Z) this.processedValue;
        }

        if (processor == null) {
            throw new NullPointerException("Internal error: Null processor in redis value");
        }

        // we execute the processor only once
        synchronized (this) {

            if (this.isProcessed) {
                return (Z) this.processedValue;
            }

            Z temp = processor.process(this);

            // reset values
            this.processedValue = temp;
            this.isProcessed = true;
        }

        return (Z) this.processedValue;
    }

    /**
     * Retrieves the TTL reference.
     *
     * @return The TTL reference time
     */
    public long getTtlReference() {
        return ttlReference;
    }

}
