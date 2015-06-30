package com.appgree.core.redis.provider;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import com.appgree.core.redis.domain.CacheKey;

/**
 * The Class RedisRemotePipelinedProvider.
 */
public class RedisRemotePipelinedProvider extends RedisRemoteBaseProvider implements RedisProvider, RedisSentinelNotifiable {

    /**
     * Instantiates a new redis remote pipelined provider.
     *
     * @throws Exception the exception
     */
    public RedisRemotePipelinedProvider() throws Exception {
        super();
    }

    /** The current jedis. */
    private static ThreadLocal<Jedis> currentJedis = new ThreadLocal<Jedis>();

    /** The current pipeline. */
    private static ThreadLocal<Pipeline> currentPipeline = new ThreadLocal<Pipeline>();

    /**
     * opens a new pipeline.
     */
    public void openPipeline() {

        if (currentPipeline.get() != null) {
            return;
        }

        Jedis jedis = currentJedis.get();
        if (jedis == null) {
            jedis = super.openJedis();
            currentJedis.set(jedis);
        }

        currentPipeline.set(jedis.pipelined());
    }

    /**
     * Returns a broken object to the pool.
     */
    public void returnBrokenPipeline() {
        currentPipeline.set(null);
        Jedis jedis = currentJedis.get();
        if (jedis == null) {
            return;
        }

        currentJedis.set(null);
        jedis.close();
    }

    /**
     * syncs the pipeline.
     */
    public void syncPipeline() {

        Pipeline pipeline = currentPipeline.get();

        if (pipeline == null) {
            return;
        }

        pipeline.sync();
        currentPipeline.set(null);

        Jedis jedis = currentJedis.get();
        if (jedis == null) {
            return;
        }

        jedis.close();
        currentJedis.set(null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisRemoteBaseProvider#poolChanged(redis.clients.jedis.JedisPool)
     */
    @Override
    public void poolChanged(JedisPool pool) {
        super.poolChanged(pool);

        // removes all old references
        currentPipeline = new ThreadLocal<Pipeline>();
        currentJedis = new ThreadLocal<Jedis>();
    }

    // redis functions

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#set(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> set(CacheKey cacheKey, String key, String value) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().set(key, value));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#get(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> get(CacheKey cacheKey, String key) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().get(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#getSet(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> getSet(CacheKey cacheKey, String key, String value) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().getSet(key, value));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hkeys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> hkeys(CacheKey cacheKey, String key) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().hkeys(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hvals(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> hvals(CacheKey cacheKey, String key) {
        return (new RedisValue<List<String>>()).setResponse(currentPipeline.get().hvals(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hset(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hset(CacheKey cacheKey, String key, String field, String value) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().hset(key, field, value));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hdel(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hdel(CacheKey cacheKey, String key, String field) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().hdel(key, field));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmset(dw.cache.provider.CacheKey, java.lang.String, java.util.Map)
     */
    @Override
    public RedisValue<String> hmset(CacheKey cacheKey, String key, Map<String, String> hash) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().hmset(key, hash));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> hget(CacheKey cacheKey, String key, String field) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().hget(key, field));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<List<String>> hmget(CacheKey cacheKey, String key, String... fields) {
        return (new RedisValue<List<String>>()).setResponse(currentPipeline.get().hmget(key, fields));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hgetAll(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Map<String, String>> hgetAll(CacheKey cacheKey, String key) {
        return (new RedisValue<Map<String, String>>()).setResponse(currentPipeline.get().hgetAll(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hincrBy(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> hincrBy(CacheKey cacheKey, String key, String field, int value) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().hincrBy(key, field, value));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hexists(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> hexists(CacheKey cacheKey, String key, String field) {
        return (new RedisValue<Boolean>()).setResponse(currentPipeline.get().hexists(key, field));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hlen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> hlen(CacheKey cacheKey, String key) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().hlen(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ping(dw.cache.provider.CacheKey)
     */
    @Override
    public RedisValue<String> ping(CacheKey cacheKey) {
        RedisValue<String> value = new RedisValue<String>();
        Pipeline pipeLine = currentPipeline.get();
        Response<String> resp = pipeLine.ping();
        value.setResponse(resp);
        return value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#expire(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> expire(CacheKey cacheKey, String key, int seconds) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().expire(key, seconds));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#keys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> keys(CacheKey cacheKey, String pattern) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().keys(pattern));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#del(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Long> del(CacheKey cacheKey, String... keys) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().del(keys));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#exists(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> exists(CacheKey cacheKey, String key) {
        return (new RedisValue<Boolean>()).setResponse(currentPipeline.get().exists(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#incr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> incr(CacheKey cacheKey, String key) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().incr(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#decr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> decr(CacheKey cacheKey, String key) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().decr(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zadd(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Long> zadd(CacheKey cacheKey, String key, double score, String member) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zadd(key, score, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zincrby(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Double> zincrby(CacheKey cacheKey, String key, double score, String member) {
        return (new RedisValue<Double>()).setResponse(currentPipeline.get().zincrby(key, score, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zscore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Double> zscore(CacheKey cacheKey, String key, String member) {
        return (new RedisValue<Double>()).setResponse(currentPipeline.get().zscore(key, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrank(CacheKey cacheKey, String key, String member) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zrank(key, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcount(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Long> zcount(CacheKey cacheKey, String key, double min, double max) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zcount(key, min, max));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrevrank(CacheKey cacheKey, String key, String member) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zrevrank(key, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrem(CacheKey cacheKey, String key, String member) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zrem(key, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zremrangeByRank(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Long> zremrangeByRank(CacheKey cacheKey, String key, int start, int end) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zremrangeByRank(key, start, end));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> zcard(CacheKey cacheKey, String key) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zcard(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrevrange(CacheKey cacheKey, String key, int start, int end) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().zrevrange(key, start, end));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(CacheKey cacheKey, String key, double min, double max, int offset, int count) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().zrevrangeByScore(key, min, max, offset, count));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(CacheKey cacheKey, String key, double min, double max) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().zrevrangeByScore(key, min, max));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrange(CacheKey cacheKey, String key, int start, int end) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().zrange(key, start, end));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeWithScores(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrangeWithScores(CacheKey cacheKey, String key, int start, int end) {
        return (new RedisValue<Set<Tuple>>()).setResponse(currentPipeline.get().zrangeWithScores(key, start, end));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrangeByScore(CacheKey cacheKey, String key, double min, double max, int offset, int count) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().zrangeByScore(key, min, max, offset, count));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrangeByScoreWithScores(CacheKey cacheKey, String key, double min, double max, int offset, int count) {
        return (new RedisValue<Set<Tuple>>()).setResponse(currentPipeline.get().zrangeByScoreWithScores(key, min, max, offset, count));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrevrangeByScoreWithScores(CacheKey cacheKey, String key, double max, double min, int offset, int count) {
        return (new RedisValue<Set<Tuple>>()).setResponse(currentPipeline.get().zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zinterstore(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.ZParams,
     * java.lang.String[])
     */
    @Override
    public RedisValue<Long> zinterstore(CacheKey cacheKey, String dstkey, ZParams params, String... sets) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().zinterstore(dstkey, params, sets));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Set<String>> zrangeByScore(CacheKey cacheKey, String key, double min, double max) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().zrangeByScore(key, min, max));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#mget(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<List<String>> mget(CacheKey cacheKey, String... keys) {
        return (new RedisValue<List<String>>()).setResponse(currentPipeline.get().mget(keys));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> rpush(CacheKey cacheKey, String key, String... values) {
        Response<Long> res = new Response<Long>(null);
        for (String value : values) {
            res = currentPipeline.get().rpush(key, value);
        }
        return (new RedisValue<Long>()).setResponse(res);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpoplpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> rpoplpush(CacheKey cacheKey, String srckey, String dstkey) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().rpoplpush(srckey, dstkey));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> lpush(CacheKey cacheKey, String key, String... values) {
        Response<Long> res = new Response<Long>(null);
        for (String value : values) {
            res = currentPipeline.get().lpush(key, value);
        }
        return (new RedisValue<Long>()).setResponse(res);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpop(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> lpop(CacheKey cacheKey, String key) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().lpop(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#brpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> brpop(CacheKey cacheKey, int secondsBlock, String key) {
        // there is no way to simulate a blocking operation nor caching or pipelining
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#blpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> blpop(CacheKey cacheKey, int secondsBlock, String key) {
        // there is no way to simulate a blocking operation nor caching or pipelining
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lset(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<String> lset(CacheKey cacheKey, String key, int index, String value) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().lset(key, index, value));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<List<String>> lrange(CacheKey cacheKey, String key, int start, int end) {
        return (new RedisValue<List<String>>()).setResponse(currentPipeline.get().lrange(key, start, end));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lindex(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<String> lindex(CacheKey cacheKey, String key, int index) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().lindex(key, index));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrem(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<Long> lrem(CacheKey cacheKey, String key, int count, String value) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().lrem(key, count, value));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#llen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> llen(CacheKey cacheKey, String key) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().llen(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ltrim(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<String> ltrim(CacheKey cacheKey, String key, int start, int end) {
        return (new RedisValue<String>()).setResponse(currentPipeline.get().ltrim(key, start, end));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sadd(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sadd(CacheKey cacheKey, String key, String... members) {
        Response<Long> res = new Response<Long>(null);
        for (String member : members) {
            res = currentPipeline.get().sadd(key, member);
        }
        return (new RedisValue<Long>()).setResponse(res);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#srem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> srem(CacheKey cacheKey, String key, String... members) {
        Response<Long> res = new Response<Long>(null);
        for (String member : members) {
            res = currentPipeline.get().srem(key, member);
        }
        return (new RedisValue<Long>()).setResponse(res);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sismember(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> sismember(CacheKey cacheKey, String key, String member) {
        return (new RedisValue<Boolean>()).setResponse(currentPipeline.get().sismember(key, member));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#smembers(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> smembers(CacheKey cacheKey, String key) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().smembers(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#scard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> scard(CacheKey cacheKey, String key) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().scard(key));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinterstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sinterstore(CacheKey cacheKey, String dstkey, String... sets) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().sinterstore(dstkey, sets));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinter(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Set<String>> sinter(CacheKey cacheKey, String... sets) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().sinter(sets));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunionstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sunionstore(CacheKey cacheKey, String dstkey, String... sets) {
        return (new RedisValue<Long>()).setResponse(currentPipeline.get().sunionstore(dstkey, sets));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunion(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Set<String>> sunion(CacheKey cacheKey, String... sets) {
        return (new RedisValue<Set<String>>()).setResponse(currentPipeline.get().sunion(sets));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sort(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.SortingParams)
     */
    @Override
    public RedisValue<List<String>> sort(CacheKey cacheKey, String key, SortingParams sortingParameters) {
        return (new RedisValue<List<String>>()).setResponse(currentPipeline.get().sort(key, sortingParameters));
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#evalsha(dw.cache.provider.CacheKey, java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public RedisValue<Object> evalsha(CacheKey cacheKey, String sha1, List<String> keys, List<String> args) {
        throw new UnsupportedOperationException();
    }

}
