# Appgree-Redis-Wrappers

These are classes used by Appgree to encapsulate and improve a Jedis client adding pipelining functionality, master-slave control, 
and a local in-memory cache to reduce the need for different threads to repeat reads. The classes can be used independently or nested.
 
## License 

The software stands under Apache 2 License and comes with NO WARRANTY

## Features

 * RedisManager: singleton for Redis access.
 * RedisRemotePipelinedProvider: Send commands in a pipeline (independent thread pool) drastically improving throughput.
 * RedisCachedProvider: Store queried results in local cache so common access are already locally stored and need not be fetched.
 * RedisSentinelPool: Redis sentinel for high availability

## TODOs

 * Not all Jedis methods are implemented but classes can be easily extended

## Usage

All providers require a RedisRemoteBaseProvider or RedisRemotePipelinedProvider inside that contains a Jedis client or JedisPool.

 ```java
    	JedisPoolConfig config = new JedisPoolConfig();
    	// set pool configuration...
    	
    	Set<String> sentinels = new HashSet<String>(); 
    	// add sentinels. Each sentinel is defined as "<host>:<port>", you can have as many as configured
    	
        // Check sentinels configuration
        RedisSentinelPool sentinelPool = null;
        if (sentinels != null && sentinels.size() > 0) {
            sentinelPool = new RedisSentinelPool(config, sentinels, poolTimeout);
        }

        // Initializes Redis Pipelines
        RedisPipelinedManager.staticInit(maxNPipelineChunk, maxNPipelinePerConn, pipelineMaxSyncTime, cachedErrorRetryDelay,
                        maxWaitTimeout, nThreadsCallback, maxBlockingPipelineTotal, traceDelay);

     	// create the RedisRemoteProvider, in this case, all are RedisRemotePipelinedProvider

        RedisRemotePipelinedProvider pipelinedProvider = new RedisRemotePipelinedProvider();
        RedisRemoteProvider noCachedProvider = new RedisRemoteProvider(cachedErrorRetryDelay);

        // Lets initialize jedis pool
        if (sentinelPool != null) {
            sentinelPool.init(host, pipelinedProvider);
            sentinelPool.init(host, noCachedProvider);
        } else {

            // Initializes independent providers
            JedisPool pool = new JedisPool(config, host, port, poolTimeout);
            pipelinedProvider.poolChanged(pool);
            noCachedProvider.poolChanged(pool);
        }
        
        // init inner provider
        ((RedisPipelinedManager)RedisPipelinedManager.getInstance()).init(pipelinedProvider, 0);

        // Initializes Redis Manager
        RedisProvider cachedProvider = new RedisCachedProvider(RedisPipelinedManager.getInstance(), maxTtl, maxElements);
        RedisManager.staticInit(cachedProvider);

        CacheKey.staticInit(defaultTtl);
```

## Build

via Maven