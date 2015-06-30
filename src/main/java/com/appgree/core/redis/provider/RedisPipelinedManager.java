package com.appgree.core.redis.provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import com.appgree.core.redis.domain.CacheKey;

/**
 * The Class RedisPipelinedManager.
 */
public class RedisPipelinedManager implements RedisProvider {

    /** The logger. */
    private static Logger logger = Logger.getLogger(RedisPipelinedManager.class.getName());

    // the pipeline provider
    /** The pipeline provider. */
    private RedisRemotePipelinedProvider pipelineProvider;

    // the executor pool
    /** The callback workers. */
    private ThreadPoolExecutor callbackWorkers;

    // this boolean indicates if we are executing inside a worker threads
    /** The is working thread. */
    private static ThreadLocal<Boolean> isWorkingThread = new ThreadLocal<Boolean>();

    // pipeline interface
    /**
     * The Class PipelineCommand.
     *
     * @param <T> the generic type
     */
    public abstract class PipelineCommand<T> {

        /** The value. */
        private RedisValue<T> value;

        /** The response. */
        private Response<T> response;

        /**
         * Instantiates a new pipeline command.
         *
         * @param value the value
         */
        public PipelineCommand(RedisValue<T> value) {
            this.value = value;
        }

        /**
         * Execute pipeline.
         */
        public void executePipeline() {
            this.value.setFlushed();
            response = run();
        }

        /**
         * Gets the value.
         *
         * @return the value
         */
        public RedisValue<T> getValue() {
            return this.value;
        }

        /**
         * Run.
         *
         * @return the response
         */
        public abstract Response<T> run();

        /**
         * Apply value.
         */
        public void applyValue() {
            synchronized (value) {
                value.setValue(response != null ? response.get() : null);
            }
        }
    }

    /**
     * The pipeline monitor.
     */
    public static class PipelineMonitor {

        /**
         * The Enum Mode.
         */
        public enum Mode {

            /** The time. */
            TIME,
            /** The max per redis. */
            MAX_PER_REDIS,
            /** The flush. */
            FLUSH,
            /** The max total. */
            MAX_TOTAL
        };

        /** The counters. */
        private long[] counters;

        /** The mode. */
        private Mode mode = Mode.TIME;

        /** The waiting. */
        private boolean waiting = false;

        /** The trace delay. */
        private long traceDelay;

        /** The trace time. */
        private long traceTime = 0;

        /** The do trace. */
        private boolean doTrace = false;

        /** The counter sum. */
        private long counterSum = 0;

        /** The n counter sum. */
        private int nCounterSum = 0;

        /** The pending process. */
        private boolean pendingProcess = false;

        /** The trace info. */
        private String traceInfo;

        /**
         * Instantiates a new pipeline monitor.
         *
         * @param traceDelay the trace delay
         */
        public PipelineMonitor(long traceDelay) {
            this.traceDelay = traceDelay;
            this.doTrace = (this.traceDelay > 0);

            if (this.doTrace) {
                counters = new long[Mode.values().length];
            }
        }

        /**
         * Process.
         *
         * @param mode the mode
         */
        public synchronized void process(Mode mode) {

            if (this.pendingProcess == false) {
                this.mode = mode;
                this.pendingProcess = true;
            }

            if (waiting) {
                notify();
            }
        }

        /**
         * Sets the trace info.
         *
         * @param traceInfo the new trace info
         */
        private void setTraceInfo(String traceInfo) {
            this.traceInfo = traceInfo;
        }

        /**
         * Reset pending status.
         */
        private synchronized void reset() {
            this.pendingProcess = false;
            this.mode = Mode.TIME;
        }

        /**
         * Wait event.
         *
         * @param timeout the timeout
         * @throws InterruptedException the interrupted exception
         */
        public synchronized void waitEvent(long timeout) throws InterruptedException {
            waiting = true;
            try {
                if (this.doTrace) {
                    this.mode = Mode.TIME;
                }

                if (this.pendingProcess == false) {
                    wait(timeout);
                }

                this.pendingProcess = false;

            } finally {
                waiting = false;
            }

            if (this.doTrace) {
                trace();
            }
        }

        /**
         * Gets the do trace.
         *
         * @return the do trace
         */
        public boolean getDoTrace() {
            return this.doTrace;
        }

        /**
         * adds an external counter.
         *
         * @param counter The counter
         */
        public void addCounter(long counter) {
            if (this.doTrace == false || counter == 0) {
                return;
            }
            this.counterSum += counter;
            this.nCounterSum++;
        }

        /**
         * Trace.
         */
        private void trace() {
            ++this.counters[this.mode.ordinal()];

            if (System.currentTimeMillis() < this.traceTime) {
                return;
            }

            this.traceTime = System.currentTimeMillis() + this.traceDelay;

            dumpTrace();
        }

        /**
         * Dump trace.
         */
        private void dumpTrace() {

            StringBuffer buffer = new StringBuffer((this.traceInfo == null ? "" : this.traceInfo) + " Redis pipeline statistics: ");
            long sum = 0;
            for (int i = 0; i < counters.length; ++i) {
                buffer.append(" " + Mode.values()[i].toString() + ": " + ((this.counters[i] * 1000.0) / this.traceDelay) + "/s");
                sum += this.counters[i];
                this.counters[i] = 0;
            }

            buffer.append(" total: " + (sum * 1000.0) / this.traceDelay + "/s");

            logger.error(buffer.toString());
            if (this.nCounterSum == 0) {
                this.nCounterSum = 1;
            }

            logger.error((this.traceInfo == null ? "" : this.traceInfo) + " Redis pipeline statistics: average "
                            + ((double) this.counterSum / this.nCounterSum));

            // reset counters
            this.counterSum = 0;
            this.nCounterSum = 0;
        }

    }

    // the pipeline monitor to sync threads
    /** The pipeline monitor. */
    private PipelineMonitor pipelineMonitor;

    // the queue of pending commands
    /** The pipeline commands. */
    private LinkedBlockingQueue<PipelineCommand<?>> pipelineCommands = new LinkedBlockingQueue<PipelineCommand<?>>();

    // the queue for async callback execution
    /** The call back queue. */
    private LinkedBlockingQueue<Runnable> callBackQueue = new LinkedBlockingQueue<Runnable>();

    // pipelining parameters
    /** The max n pipeline chunk. */
    private int maxNPipelineChunk = 1000;

    /** The pipeline max sync time. */
    private long pipelineMaxSyncTime = 200;

    /** The pipeline error retry. */
    private long pipelineErrorRetry = 1000;

    // checks if we are in recovering mode
    /** The recovering. */
    private boolean recovering = false;

    /**
     * Gets the recovering.
     *
     * @return the recovering
     */
    public boolean getRecovering() {
        return this.recovering;
    }

    /**
     * The Class PipelineWorker.
     */
    private class PipelineWorker implements Runnable {

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {

            logger.info("Initializing pipeliner thread");

            // saves pending commands
            ArrayList<PipelineCommand<?>> pendingCommands = new ArrayList<PipelineCommand<?>>();

            for (;;) {

                try {

                    // reset pipeline status
                    pipelineMonitor.reset();

                    // processes all elements from the pending queue
                    if (pendingCommands.size() == 0 && pipelineCommands.isEmpty()) {
                        try {
                            pipelineMonitor.waitEvent(pipelineMaxSyncTime);

                            // check if we're exiting
                            if (stopThreads) {
                                logger.info("Redis pipeline thread stopping");

                                // stop workers
                                callbackWorkers.shutdown();
                                return;
                            }

                            if (pipelineMonitor.getDoTrace()) {
                                pipelineMonitor.addCounter(pipelineCommands.size());
                            }
                            continue;
                        } catch (InterruptedException e) {
                            return;
                        }
                    }

                    if (DEBUG_REDIS_CACHE) {
                        logger.info("Redis pipeline size = " + pipelineCommands.size());
                    }

                    // prepares the pipeline
                    pipelineProvider.openPipeline();

                    // processes no more than a fixed length command stream
                    // if no pending commands we get new ones
                    if (pendingCommands.size() == 0) {
                        boolean pending = true;
                        for (int i = 0; i < maxNPipelineChunk; ++i) {
                            PipelineCommand<?> command = pipelineCommands.poll();
                            if (command == null) {
                                pending = false;
                                break;
                            }
                            pendingCommands.add(command);
                            command.executePipeline();
                        }

                        if (DEBUG_REDIS_CACHE) {
                            if (pending) {
                                logger.info("Pipeline overflow detected with " + maxNPipelineChunk + " elements in server number " + serverNumber);
                            }
                        }
                    } else {
                        // if we have pending commands we just try to execute them again
                        for (PipelineCommand<?> command : pendingCommands) {
                            command.executePipeline();
                        }
                    }

                    // syncs the pipeline
                    pipelineProvider.syncPipeline();

                    // ok, now we can notify values
                    for (final PipelineCommand<?> command : pendingCommands) {
                        callbackWorkers.execute(new Runnable() {

                            @Override
                            public void run() {
                                if (isWorkingThread.get() == null) {
                                    isWorkingThread.set(true);
                                }
                                command.applyValue();
                            }
                        });
                    }

                    // command ok, leaves recovering mode
                    if (recovering) {
                        recovering = false;
                    }

                    // ok, all commands dispatched
                    pendingCommands.clear();
                } catch (Throwable e) {

                    // ups, error executing the pipeline.... we have a broken pipeline
                    try {
                        pipelineProvider.returnBrokenPipeline();
                    } catch (Throwable eRerturn) {
                        logger.error("Error returning broken pipeline to pool", eRerturn);
                    }

                    // sets recovering mode
                    if (recovering == false) {
                        recovering = true;
                    }

                    logger.error("Error processing pipeline. Sleeping for " + pipelineErrorRetry + "ms", e);
                    try {
                        Thread.sleep(pipelineErrorRetry);
                    } catch (InterruptedException e1) {
                        return;
                    }
                }
            }
        }
    }

    // the walking dead... sorry, I'd say working thread... very similar, don't you think?
    /** The pipeline worker thread. */
    private Thread pipelineWorkerThread;

    // the server number
    /** The server number. */
    private int serverNumber;

    // the number of threads for callback
    /** The n threads callback. */
    private int nThreadsCallback;

    // the max number of entries per connection
    /** The max n pipeline per conn. */
    private int maxNPipelinePerConn;

    // the max wait timeout
    /** The max wait timeout. */
    private static long maxWaitTimeout;

    // the max number of entries for all connections
    /** The max blocking pipeline total. */
    private static int maxBlockingPipelineTotal;

    // the total number of commands for all pipelines for all servers
    /** The total blocking pipelined. */
    private static AtomicInteger totalBlockingPipelined = new AtomicInteger();

    // Singleton pattern
    /** The instances. */
    private static RedisProvider instance = null;

    /**
     * Gets the instance.
     *
     * @return the instance
     */
    public static RedisProvider getInstance() {
        return instance;
    }

    // indicates that server threads should stop
    /** The stop threads. */
    private boolean stopThreads = false;

    // indicates we're in shutdown mode
    /** The shutdown. */
    private boolean shutdown = false;

    // Private constructor prevents instantiation from other classes
    /**
     * Instantiates a new redis pipelined manager.
     *
     * @param maxNPipelineChunk the max n pipeline chunk
     * @param maxNPipelinePerConn the max n pipeline per conn
     * @param pipelineMaxSyncTime the pipeline max sync time
     * @param pipelineErrorRetry the pipeline error retry
     * @param nThreadsCallback the n threads callback
     * @param traceDelay the trace delay
     */
    private RedisPipelinedManager(int maxNPipelineChunk, int maxNPipelinePerConn, long pipelineMaxSyncTime, long pipelineErrorRetry,
                    int nThreadsCallback, long traceDelay) {

        this.maxNPipelineChunk = maxNPipelineChunk;
        this.maxNPipelinePerConn = maxNPipelinePerConn;
        this.pipelineMaxSyncTime = pipelineMaxSyncTime;
        this.pipelineErrorRetry = pipelineErrorRetry;
        this.nThreadsCallback = nThreadsCallback;
        this.pipelineMonitor = new PipelineMonitor(traceDelay);

        // initializes the working pool
        callbackWorkers = new ThreadPoolExecutor(this.nThreadsCallback, this.nThreadsCallback, Integer.MAX_VALUE, TimeUnit.MILLISECONDS,
                        callBackQueue);
        callbackWorkers.setRejectedExecutionHandler(new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.error("Error processing task in worker thread!");
            }
        });

        // stats working thread
        pipelineWorkerThread = new Thread(new PipelineWorker());
        pipelineWorkerThread.start();
    }

    /**
     * Gets the max wait timeout.
     *
     * @return the max wait timeout
     */
    public static long getMaxWaitTimeout() {
        return maxWaitTimeout;
    }

    /**
     * Static init.
     *
     * @param maxNPipelineChunk the max n pipeline chunk
     * @param maxNPipelinePerConn the max n pipeline per conn
     * @param pipelineMaxSyncTime the pipeline max sync time
     * @param pipelineErrorRetry the pipeline error retry
     * @param maxWaitTimeout the max wait timeout
     * @param nThreadsCallback the n threads callback
     * @param maxBlockingPipelineTotal the max blocking pipeline total
     * @param traceDelay the trace delay
     */
    public static void staticInit(int maxNPipelineChunk, int maxNPipelinePerConn, long pipelineMaxSyncTime, long pipelineErrorRetry,
                    long maxWaitTimeout, int nThreadsCallback, int maxBlockingPipelineTotal, long traceDelay) {
        RedisPipelinedManager.maxWaitTimeout = maxWaitTimeout;
        RedisPipelinedManager.maxBlockingPipelineTotal = maxBlockingPipelineTotal;

        instance = new RedisPipelinedManager(maxNPipelineChunk, maxNPipelinePerConn, pipelineMaxSyncTime, pipelineErrorRetry, nThreadsCallback,
                        traceDelay);

    }

    /**
     * Uninit all.
     */
    public static void uninitAll() {

        if (instance == null) {
            return;
        }

        if (instance instanceof RedisPipelinedManager) {
            ((RedisPipelinedManager) instance).uninit();
        }
        instance = null;
    }

    /**
     * Uninit.
     */
    private void uninit() {
        this.stopThreads = true;
        this.shutdown = true;
    }

    /**
     * Inits the.
     *
     * @param pipelineProvider the pipeline provider
     * @param number the number
     */
    public void init(RedisRemotePipelinedProvider pipelineProvider, int number) {
        logger.info("Initializing Redis Cached Manager: " + number);
        this.pipelineProvider = pipelineProvider;
        this.serverNumber = number;
        this.pipelineMonitor.setTraceInfo("Server " + number);
    }

    /**
     * Generates a string concat.
     *
     * @param methodId The method id
     * @param fields The method keys
     * @return The concat
     */
    private static String concat(String methodId, String... fields) {
        if (DEBUG_REDIS_CACHE) {
            StringBuilder builder = new StringBuilder();
            builder.append(methodId);
            for (String item : fields) {
                builder.append("[");
                builder.append(item == null ? "nil" : item);
            }

            logger.info("CACHEPIPE: " + builder.toString());
            return builder.toString();
        }

        return "";
    }

    /**
     * Appends a pipeline command.
     *
     * @param command the command
     */
    private void appendCommand(PipelineCommand<?> command) {
        command.getValue().setPipelinedManager(this);
        this.pipelineCommands.offer(command);
        int current = this.pipelineCommands.size();
        if (current >= this.maxNPipelinePerConn) {
            if (DEBUG_REDIS_CACHE) {
                logger.info("CACHEPIPE: flushing pipeline number of queued elements: " + current);
            }
            this.pipelineMonitor.process(PipelineMonitor.Mode.MAX_PER_REDIS);
        }
    }

    /**
     * Incr blocking counter.
     *
     * @param incr the incr
     */
    static void incrBlockingCounter(int incr) {

        // if we're inside a working thread we don't compute blocking elements
        // we only compute container working threads
        if (isWorkingThread.get() != null) {
            return;
        }

        synchronized (totalBlockingPipelined) {

            int current = totalBlockingPipelined.get() + incr;

            if (current < 0) {
                current = 0;
            }

            if (current >= maxBlockingPipelineTotal) {

                if (DEBUG_REDIS_CACHE) {
                    logger.info("CACHEPIPE: flushing pipeline number of total blocking elements: " + current);
                }

                // sends events to all managers
                notifyMannager();

                // reset counters
                current = 0;
            }

            totalBlockingPipelined.set(current);
        }
    }

    /**
     * Sends an event to all managers.
     */
    private static void notifyMannager() {
        if (instance instanceof RedisPipelinedManager) {
            ((RedisPipelinedManager) instance).pipelineMonitor.process(PipelineMonitor.Mode.MAX_TOTAL);
        }
    }

    /**
     * Flush.
     */
    void flush() {
        this.pipelineMonitor.process(PipelineMonitor.Mode.FLUSH);
    }

    /**
     * Checks if is shutdown.
     *
     * @return true, if is shutdown
     */
    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * ------------------------------------------------------------------ Redis implementation
     * ------------------------------------------------------------------.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param value the value
     * @return the redis value
     */

    @Override
    public RedisValue<String> set(final CacheKey cacheKey, final String key, final String value) {
        concat("set", key, value);
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.set(cacheKey, key, value)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#get(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> get(final CacheKey cacheKey, final String key) {
        concat("get", key);
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.get(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#getSet(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> getSet(final CacheKey cacheKey, final String key, final String value) {
        concat("getSet", key);
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.getSet(cacheKey, key, value)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hset(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hset(final CacheKey cacheKey, final String key, final String field, final String value) {
        concat("hset", key, field, value);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.hset(cacheKey, key, field, value)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hdel(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hdel(final CacheKey cacheKey, final String key, final String field) {
        concat("hdel", key, field);

        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.hdel(cacheKey, key, field)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmset(dw.cache.provider.CacheKey, java.lang.String, java.util.Map)
     */
    @Override
    public RedisValue<String> hmset(final CacheKey cacheKey, final String key, final Map<String, String> hash) {
        concat("hmset", key, hash.toString());
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.hmset(cacheKey, key, hash)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> hget(final CacheKey cacheKey, final String key, final String field) {
        concat("hget", key, field);
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.hget(cacheKey, key, field)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<List<String>> hmget(final CacheKey cacheKey, final String key, final String... fields) {
        String[] fieldList = new String[fields.length + 1];
        fieldList[0] = key;
        for (int i = 0; i < fields.length; i++) {
            fieldList[i + 1] = fields[i];
        }
        concat("hmget", fieldList);
        RedisValue<List<String>> valueImpl = new RedisValue<List<String>>();

        appendCommand(new PipelineCommand<List<String>>(valueImpl) {

            @Override
            public Response<List<String>> run() {
                return ((RedisValue<List<String>>) pipelineProvider.hmget(cacheKey, key, fields)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hgetAll(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Map<String, String>> hgetAll(final CacheKey cacheKey, final String key) {
        concat("hgetAll", key);
        RedisValue<Map<String, String>> valueImpl = new RedisValue<Map<String, String>>();

        appendCommand(new PipelineCommand<Map<String, String>>(valueImpl) {

            @Override
            public Response<Map<String, String>> run() {
                return ((RedisValue<Map<String, String>>) pipelineProvider.hgetAll(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hexists(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> hexists(final CacheKey cacheKey, final String key, final String field) {
        concat("hexists", key, field);
        RedisValue<Boolean> valueImpl = new RedisValue<Boolean>();

        appendCommand(new PipelineCommand<Boolean>(valueImpl) {

            @Override
            public Response<Boolean> run() {
                return ((RedisValue<Boolean>) pipelineProvider.hexists(cacheKey, key, field)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hlen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> hlen(final CacheKey cacheKey, final String key) {
        concat("hlen", key);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.hlen(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hincrBy(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> hincrBy(final CacheKey cacheKey, final String key, final String field, final int value) {
        concat("hincrBy", key, field, String.valueOf(value));
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.hincrBy(cacheKey, key, field, value)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ping(dw.cache.provider.CacheKey)
     */
    @Override
    public RedisValue<String> ping(final CacheKey cacheKey) {
        concat("ping");
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.ping(cacheKey)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#expire(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> expire(final CacheKey cacheKey, final String key, final int seconds) {
        concat("expire", key, String.valueOf(seconds));
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.expire(cacheKey, key, seconds)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#del(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Long> del(final CacheKey cacheKey, final String... keys) {
        concat("del", keys);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.del(cacheKey, keys)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#exists(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> exists(final CacheKey cacheKey, final String key) {
        concat("exists", key);
        RedisValue<Boolean> valueImpl = new RedisValue<Boolean>();

        appendCommand(new PipelineCommand<Boolean>(valueImpl) {

            @Override
            public Response<Boolean> run() {
                return ((RedisValue<Boolean>) pipelineProvider.exists(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#incr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> incr(final CacheKey cacheKey, final String key) {
        concat("incr", key);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.incr(cacheKey, key)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#decr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> decr(final CacheKey cacheKey, final String key) {
        concat("decr", key);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.decr(cacheKey, key)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#keys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> keys(final CacheKey cacheKey, final String pattern) {
        concat("keys", pattern);
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.keys(cacheKey, pattern)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zadd(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Long> zadd(final CacheKey cacheKey, final String key, final double score, final String member) {
        concat("zadd", key, String.valueOf(score), member);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zadd(cacheKey, key, score, member)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zincrby(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Double> zincrby(final CacheKey cacheKey, final String key, final double score, final String member) {
        concat("zincrBy", key, String.valueOf(score), member);
        RedisValue<Double> valueImpl = new RedisValue<Double>();

        appendCommand(new PipelineCommand<Double>(valueImpl) {

            @Override
            public Response<Double> run() {
                return ((RedisValue<Double>) pipelineProvider.zincrby(cacheKey, key, score, member)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zscore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Double> zscore(final CacheKey cacheKey, final String key, final String member) {
        concat("zscore", key, member);
        RedisValue<Double> valueImpl = new RedisValue<Double>();

        appendCommand(new PipelineCommand<Double>(valueImpl) {

            @Override
            public Response<Double> run() {
                return ((RedisValue<Double>) pipelineProvider.zscore(cacheKey, key, member)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrank(final CacheKey cacheKey, final String key, final String member) {
        concat("zrank", key, member);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zrank(cacheKey, key, member)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcount(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Long> zcount(final CacheKey cacheKey, final String key, final double min, final double max) {
        concat("zrank", key, String.valueOf(min), String.valueOf(max));
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zcount(cacheKey, key, min, max)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrevrank(final CacheKey cacheKey, final String key, final String member) {
        concat("zrevrank", key, member);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zrevrank(cacheKey, key, member)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrem(final CacheKey cacheKey, final String key, final String member) {
        concat("zrem", key, member);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zrem(cacheKey, key, member)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zremrangeByRank(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Long> zremrangeByRank(final CacheKey cacheKey, final String key, final int start, final int end) {
        concat("zremrangeByRank", key, String.valueOf(start), String.valueOf(end));
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zremrangeByRank(cacheKey, key, start, end)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> zcard(final CacheKey cacheKey, final String key) {
        concat("scard", key);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zcard(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrevrange(final CacheKey cacheKey, final String key, final int start, final int end) {
        concat("zrevrange", key, String.valueOf(start), String.valueOf(end));
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.zrevrange(cacheKey, key, start, end)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max, final int offset,
                    final int count) {
        concat("zrevrangeByScore", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.zrevrangeByScore(cacheKey, key, min, max, offset, count)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max) {
        concat("zrevrangeByScore", key, String.valueOf(min), String.valueOf(max));
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.zrevrangeByScore(cacheKey, key, min, max)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrange(final CacheKey cacheKey, final String key, final int start, final int end) {
        concat("zrange", key, String.valueOf(start), String.valueOf(end));
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.zrange(cacheKey, key, start, end)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeWithScores(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrangeWithScores(final CacheKey cacheKey, final String key, final int start, final int end) {
        concat("zrangewscores", key, String.valueOf(start), String.valueOf(end));
        RedisValue<Set<Tuple>> valueImpl = new RedisValue<Set<Tuple>>();

        appendCommand(new PipelineCommand<Set<Tuple>>(valueImpl) {

            @Override
            public Response<Set<Tuple>> run() {
                return ((RedisValue<Set<Tuple>>) pipelineProvider.zrangeWithScores(cacheKey, key, start, end)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max, final int offset,
                    final int count) {
        concat("zrangeByScore", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.zrangeByScore(cacheKey, key, min, max, offset, count)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrangeByScoreWithScores(final CacheKey cacheKey, final String key, final double min, final double max,
                    final int offset, final int count) {
        concat("zrangeByScoreWithScores", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        RedisValue<Set<Tuple>> valueImpl = new RedisValue<Set<Tuple>>();

        appendCommand(new PipelineCommand<Set<Tuple>>(valueImpl) {

            @Override
            public Response<Set<Tuple>> run() {
                return ((RedisValue<Set<Tuple>>) pipelineProvider.zrangeByScoreWithScores(cacheKey, key, min, max, offset, count)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrevrangeByScoreWithScores(final CacheKey cacheKey, final String key, final double max, final double min,
                    final int offset, final int count) {
        concat("zrevrangeByScoreWithScores", key, String.valueOf(max), String.valueOf(min), String.valueOf(offset), String.valueOf(count));
        RedisValue<Set<Tuple>> valueImpl = new RedisValue<Set<Tuple>>();

        appendCommand(new PipelineCommand<Set<Tuple>>(valueImpl) {

            @Override
            public Response<Set<Tuple>> run() {
                return ((RedisValue<Set<Tuple>>) pipelineProvider.zrevrangeByScoreWithScores(cacheKey, key, max, min, offset, count)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zinterstore(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.ZParams,
     * java.lang.String[])
     */
    @Override
    public RedisValue<Long> zinterstore(final CacheKey cacheKey, final String dstkey, final ZParams params, final String... sets) {
        int len = params.getParams().size() + sets.length + 1;
        String[] actualParams = new String[len];
        actualParams[0] = dstkey;
        int i = 1;
        for (byte[] p : params.getParams()) {
            try {
                actualParams[i++] = new String(p, "UTF-8");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        for (String set : sets) {
            actualParams[i++] = set;
        }
        concat("zinterstore", actualParams);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.zinterstore(cacheKey, dstkey, params, sets)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Set<String>> zrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max) {
        concat("zrangeByScore", key, String.valueOf(min), String.valueOf(max));
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.zrangeByScore(cacheKey, key, min, max)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#mget(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<List<String>> mget(final CacheKey cacheKey, final String... keys) {
        concat("mget", keys);
        RedisValue<List<String>> valueImpl = new RedisValue<List<String>>();

        appendCommand(new PipelineCommand<List<String>>(valueImpl) {

            @Override
            public Response<List<String>> run() {
                return ((RedisValue<List<String>>) pipelineProvider.mget(cacheKey, keys)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> rpush(final CacheKey cacheKey, final String key, final String... values) {
        if (DEBUG_REDIS_CACHE) {
            String[] strArray = new String[values.length + 1];
            strArray[0] = key;
            for (int i = 0; i < values.length; i++) {
                strArray[i + 1] = values[i];
            }
            concat("rpush", strArray);
        }

        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.rpush(cacheKey, key, values)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpoplpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> rpoplpush(final CacheKey cacheKey, final String srckey, final String dstkey) {
        concat("rpoplpush", srckey, dstkey);

        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.rpoplpush(cacheKey, srckey, dstkey)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> lpush(final CacheKey cacheKey, final String key, final String... values) {
        if (DEBUG_REDIS_CACHE) {
            String[] strArray = new String[values.length + 1];
            strArray[0] = key;
            for (int i = 0; i < values.length; i++) {
                strArray[i + 1] = values[i];
            }
            concat("lpush", strArray);
        }

        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.lpush(cacheKey, key, values)).getResponse();
            }
        });
        return valueImpl;
    }


    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpop(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> lpop(final CacheKey cacheKey, final String key) {
        concat("lpop", key, String.valueOf(key));
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.lpop(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#brpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> brpop(final CacheKey cacheKey, final int secondsBlock, final String key) {
        // there is no way to simulate a blocking operation nor caching or pipelining
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#blpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> blpop(final CacheKey cacheKey, final int secondsBlock, final String key) {
        // there is no way to simulate a blocking operation nor caching or pipelining
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lset(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<String> lset(final CacheKey cacheKey, final String key, final int index, final String value) {
        concat("lset", key, String.valueOf(index), value);

        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.lset(cacheKey, key, index, value)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<List<String>> lrange(final CacheKey cacheKey, final String key, final int start, final int end) {
        concat("lrange", key, String.valueOf(start), String.valueOf(end));
        RedisValue<List<String>> valueImpl = new RedisValue<List<String>>();

        appendCommand(new PipelineCommand<List<String>>(valueImpl) {

            @Override
            public Response<List<String>> run() {
                return ((RedisValue<List<String>>) pipelineProvider.lrange(cacheKey, key, start, end)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lindex(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<String> lindex(final CacheKey cacheKey, final String key, final int index) {
        concat("lindex", key, String.valueOf(index));
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.lindex(cacheKey, key, index)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrem(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<Long> lrem(final CacheKey cacheKey, final String key, final int count, final String value) {
        concat("lrem", key, String.valueOf(count), value);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.lrem(cacheKey, key, count, value)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ltrim(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<String> ltrim(final CacheKey cacheKey, final String key, final int start, final int end) {
        concat("ltrim", key, String.valueOf(start), String.valueOf(end));
        RedisValue<String> valueImpl = new RedisValue<String>();

        appendCommand(new PipelineCommand<String>(valueImpl) {

            @Override
            public Response<String> run() {
                return ((RedisValue<String>) pipelineProvider.ltrim(cacheKey, key, start, end)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#llen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> llen(final CacheKey cacheKey, final String key) {
        concat("llen", key);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.llen(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hkeys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> hkeys(final CacheKey cacheKey, final String key) {
        concat("hkeys", key);
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.hkeys(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hvals(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> hvals(final CacheKey cacheKey, final String key) {
        concat("hvals", key);
        RedisValue<List<String>> valueImpl = new RedisValue<List<String>>();

        appendCommand(new PipelineCommand<List<String>>(valueImpl) {

            @Override
            public Response<List<String>> run() {
                return ((RedisValue<List<String>>) pipelineProvider.hvals(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sadd(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sadd(final CacheKey cacheKey, final String key, final String... members) {
        if (DEBUG_REDIS_CACHE) {
            String[] strArray = new String[members.length + 1];
            strArray[0] = key;
            for (int i = 0; i < members.length; i++) {
                strArray[i + 1] = members[i];
            }
            concat("sadd", strArray);
        }
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.sadd(cacheKey, key, members)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#srem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> srem(final CacheKey cacheKey, final String key, final String... members) {
        if (DEBUG_REDIS_CACHE) {
            String[] memberList = new String[members.length + 1];
            memberList[0] = key;
            for (int i = 0; i < members.length; i++) {
                memberList[i + 1] = members[i];
            }
            concat("sadd", memberList);
        }

        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.srem(cacheKey, key, members)).getResponse();
            }
        });
        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sismember(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> sismember(final CacheKey cacheKey, final String key, final String member) {
        concat("sismember", key, member);
        RedisValue<Boolean> valueImpl = new RedisValue<Boolean>();

        appendCommand(new PipelineCommand<Boolean>(valueImpl) {

            @Override
            public Response<Boolean> run() {
                return ((RedisValue<Boolean>) pipelineProvider.sismember(cacheKey, key, member)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#smembers(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> smembers(final CacheKey cacheKey, final String key) {
        concat("smembers", key);
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.smembers(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#scard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> scard(final CacheKey cacheKey, final String key) {
        concat("scard", key);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.scard(cacheKey, key)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinterstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sinterstore(final CacheKey cacheKey, final String dstkey, final String... sets) {
        String[] setList = new String[sets.length + 1];
        setList[0] = dstkey;
        for (int i = 0; i < sets.length; i++) {
            setList[i + 1] = sets[i];
        }
        concat("sinterstore", setList);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.sinterstore(cacheKey, dstkey, sets)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinter(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Set<String>> sinter(final CacheKey cacheKey, final String... sets) {
        concat("sinter", sets);
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.sinter(cacheKey, sets)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunionstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sunionstore(final CacheKey cacheKey, final String dstkey, final String... sets) {
        String[] setList = new String[sets.length + 1];
        setList[0] = dstkey;
        for (int i = 0; i < sets.length; i++) {
            setList[i + 1] = sets[i];
        }
        concat("sunionstore", setList);
        RedisValue<Long> valueImpl = new RedisValue<Long>();

        appendCommand(new PipelineCommand<Long>(valueImpl) {

            @Override
            public Response<Long> run() {
                return ((RedisValue<Long>) pipelineProvider.sunionstore(cacheKey, dstkey, sets)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunion(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Set<String>> sunion(final CacheKey cacheKey, final String... sets) {
        concat("sunion", sets);
        RedisValue<Set<String>> valueImpl = new RedisValue<Set<String>>();

        appendCommand(new PipelineCommand<Set<String>>(valueImpl) {

            @Override
            public Response<Set<String>> run() {
                return ((RedisValue<Set<String>>) pipelineProvider.sunion(cacheKey, sets)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sort(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.SortingParams)
     */
    @Override
    public RedisValue<List<String>> sort(final CacheKey cacheKey, final String key, final SortingParams sortingParameters) {
        Collection<byte[]> params = sortingParameters.getParams();
        String[] actualParams = new String[params.size() + 1];
        actualParams[0] = key;
        int i = 1;
        for (byte[] p : params) {
            try {
                actualParams[i++] = new String(p, "UTF-8");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        concat("sort", actualParams);
        RedisValue<List<String>> valueImpl = new RedisValue<List<String>>();

        appendCommand(new PipelineCommand<List<String>>(valueImpl) {

            @Override
            public Response<List<String>> run() {
                return ((RedisValue<List<String>>) pipelineProvider.sort(cacheKey, key, sortingParameters)).getResponse();
            }
        });

        return valueImpl;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#evalsha(dw.cache.provider.CacheKey, java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public RedisValue<Object> evalsha(final CacheKey cacheKey, final String sha1, final List<String> keys, final List<String> args) {
        concat("evalsha", sha1);
        RedisValue<Object> valueImpl = new RedisValue<Object>();

        appendCommand(new PipelineCommand<Object>(valueImpl) {

            @Override
            public Response<Object> run() {
                return ((RedisValue<Object>) pipelineProvider.evalsha(cacheKey, sha1, keys, args)).getResponse();
            }
        });
        return valueImpl;
    }

}
