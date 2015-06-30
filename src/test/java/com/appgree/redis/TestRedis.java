package com.appgree.redis;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.appgree.core.redis.domain.CacheKey;
import com.appgree.core.redis.provider.RedisCachedProvider;
import com.appgree.core.redis.provider.RedisManager;
import com.appgree.core.redis.provider.RedisPipelinedManager;
import com.appgree.core.redis.provider.RedisProvider;
import com.appgree.core.redis.provider.RedisRemotePipelinedProvider;
import com.appgree.core.redis.provider.RedisRemoteProvider;
import com.appgree.core.redis.provider.RedisSentinelPool;

public class TestRedis {

    private static Logger logger = Logger.getLogger(TestRedis.class.getName());
    private static RedisProvider provider;

    @BeforeClass
    public static void init() throws Exception {

        logger.debug("Initializing Redis test");

        // initializes redis pool
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(5);
        config.setMaxIdle(2);
        config.setMinIdle(1);
        config.setMaxWaitMillis(1000);
        config.setBlockWhenExhausted(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTimeBetweenEvictionRunsMillis(500);
        config.setMinEvictableIdleTimeMillis(-1);
        config.setTestWhileIdle(true);
        config.setSoftMinEvictableIdleTimeMillis(5000);
        config.setNumTestsPerEvictionRun(1);


        Set<String> sentinels = new HashSet<String>();
        // add sentinels. Each sentinel is defined as "<host>:<port>", you can have as many as configured

        // Check sentinels configuration
        RedisSentinelPool sentinelPool = null;
        if (sentinels != null && sentinels.size() > 0) {
            sentinelPool = new RedisSentinelPool(config, sentinels, 200);
        }

        // Initializes Redis Pipelines
        RedisPipelinedManager.staticInit(1000, 10, 5, 5000, 15000, 20, 20, 0);

        // create the RedisRemoteProvider, in this case, all are RedisRemotePipelinedProvider

        RedisRemotePipelinedProvider pipelinedProvider = new RedisRemotePipelinedProvider();
        RedisRemoteProvider noCachedProvider = new RedisRemoteProvider(5000);

        // Lets initialize jedis pool
        if (sentinelPool != null && sentinels.size() > 0) {
            sentinelPool.init("127.0.0.1", pipelinedProvider);
            sentinelPool.init("127.0.0.1", noCachedProvider);
        } else {

            // Initializes independent providers
            JedisPool pool = new JedisPool(config, "127.0.0.1", 6379, 3000);
            pipelinedProvider.poolChanged(pool);
            noCachedProvider.poolChanged(pool);
        }

        ((RedisPipelinedManager) RedisPipelinedManager.getInstance()).init(pipelinedProvider, 0);

        // Initializes Redis Manager
        RedisProvider cachedProvider = new RedisCachedProvider(RedisPipelinedManager.getInstance(), 61000, 10000);
        RedisManager.staticInit(cachedProvider);

        provider = RedisManager.getInstance();

    }

    @AfterClass
    public static void destroy() {
        RedisManager.uninitAll();
    }

    @Test
    public void readwrite() throws Exception {

        CacheKey key = new CacheKey("test", true);

        String pong = provider.ping(key).get();

        Assert.assertEquals(pong, "PONG");

        provider.del(key, key.getName() + "-1");

        String res = provider.get(key, key.getName() + "-1").get();

        Assert.assertNull(res);

        provider.set(key, key.getName() + "-2", "value1");

        res = provider.get(key, key.getName() + "-2").get();

        Assert.assertEquals(res, "value1");

        provider.del(key, key.getName() + "-2");

        provider.hset(key, key.getName() + "-hash", "field1", "value-3");

        res = provider.hget(key, key.getName() + "-hash", "field1").get();

        Assert.assertEquals(res, "value-3");

        provider.del(key, key.getName() + "-hash");

    }

    // @Test
    public void TestSortedSets() throws Exception {

        String pong = provider.ping(null).get();

        Assert.assertEquals(pong, "PONG");

        Random rng = new Random(512);
        long startTmst = System.currentTimeMillis();

        int small = 2000;
        int medium = 20000;
        int big = 200000;

        int[] rand = new int[big];
        for (int i = 0; i < big; i++) {
            rand[i] = rng.nextInt(Integer.MAX_VALUE);
        }
        long nextTmst = System.currentTimeMillis();
        System.out.println("Rnd initialization in " + (nextTmst - startTmst));

        String[] set = new String[big];
        for (int i = 0; i < set.length; i++) {
            String current = String.valueOf(i);
            set[i] = current;
        }

        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("Arrays initialization in " + (nextTmst - startTmst));

        for (int i = 0; i < small; i++) {
            provider.zadd(null, "testSetSmallAsc", i, set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        for (int i = 0; i < small; i++) {
            provider.zadd(null, "testSetSmallDesc", small - i, set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        for (int i = 0; i < small; i++) {
            provider.zadd(null, "testSetSmallRnd", rand[i], set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        System.out.println(" --- ");

        for (int i = 0; i < medium; i++) {
            provider.zadd(null, "testSetMediumAsc", i, set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        for (int i = 0; i < medium; i++) {
            provider.zadd(null, "testSetMediumDesc", medium - i, set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        for (int i = 0; i < medium; i++) {
            provider.zadd(null, "testSetSMediumRnd", rand[i], set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        System.out.println(" --- ");

        for (int i = 0; i < big; i++) {
            provider.zadd(null, "testSetBigAsc", i, set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        for (int i = 0; i < big; i++) {
            provider.zadd(null, "testSetBigDesc", big - i, set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));

        for (int i = 0; i < big; i++) {
            provider.zadd(null, "testSetSBigRnd", rand[i], set[i]).get();
        }
        startTmst = nextTmst;
        nextTmst = System.currentTimeMillis();
        System.out.println("" + (nextTmst - startTmst));


    }
}
