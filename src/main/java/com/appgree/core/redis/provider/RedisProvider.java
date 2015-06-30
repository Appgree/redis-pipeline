package com.appgree.core.redis.provider;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import com.appgree.core.redis.domain.CacheKey;

/**
 * The Interface RedisProvider.
 */
public interface RedisProvider {

    /** The Constant DEBUG_REDIS_CACHE. */
    public static final boolean DEBUG_REDIS_CACHE = false;

    // key functions
    /**
     * Sets the.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param value the value
     * @return the redis value
     */
    public RedisValue<String> set(CacheKey cacheKey, String key, String value);

    /**
     * Gets the.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<String> get(CacheKey cacheKey, String key);

    /**
     * Gets the sets the.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param value the value
     * @return the sets the
     */
    public RedisValue<String> getSet(CacheKey cacheKey, String key, String value);

    /**
     * Hkeys.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Set<String>> hkeys(CacheKey cacheKey, String key);

    /**
     * Hvals.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<List<String>> hvals(CacheKey cacheKey, String key);

    /**
     * Keys.
     *
     * @param cacheKey the cache key
     * @param pattern the pattern
     * @return the redis value
     */
    @Deprecated
    /** should not be used in production code, keys is for debugging only, redis 2.8 now supports - scan - for production */
    public RedisValue<Set<String>> keys(CacheKey cacheKey, String pattern);

    /**
     * Del.
     *
     * @param cacheKey the cache key
     * @param keys the keys
     * @return the redis value
     */
    public RedisValue<Long> del(CacheKey cacheKey, String... keys);

    /**
     * Exists.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Boolean> exists(CacheKey cacheKey, String key);

    /**
     * Incr.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Long> incr(CacheKey cacheKey, String key);

    /**
     * Decr.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Long> decr(CacheKey cacheKey, String key);

    // hash functions
    /**
     * Hset.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param field the field
     * @param value the value
     * @return the redis value
     */
    public RedisValue<Long> hset(CacheKey cacheKey, String key, String field, String value);

    /**
     * Hdel.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param field the field
     * @return the redis value
     */
    public RedisValue<Long> hdel(CacheKey cacheKey, String key, String field);

    /**
     * Hmset.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param hash the hash
     * @return the redis value
     */
    public RedisValue<String> hmset(CacheKey cacheKey, String key, Map<String, String> hash);

    /**
     * Hget.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param field the field
     * @return the redis value
     */
    public RedisValue<String> hget(CacheKey cacheKey, String key, String field);

    /**
     * Hmget.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param fields the fields
     * @return the redis value
     */
    public RedisValue<List<String>> hmget(CacheKey cacheKey, String key, String... fields);

    /**
     * Hget all.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Map<String, String>> hgetAll(CacheKey cacheKey, String key);

    /**
     * Hincr by.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param field the field
     * @param value the value
     * @return the redis value
     */
    public RedisValue<Long> hincrBy(CacheKey cacheKey, String key, String field, int value);

    /**
     * Hexists.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param field the field
     * @return the redis value
     */
    public RedisValue<Boolean> hexists(CacheKey cacheKey, String key, String field);

    /**
     * Hlen.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Long> hlen(CacheKey cacheKey, String key);

    // utilities
    /**
     * Ping.
     *
     * @param cacheKey the cache key
     * @return the redis value
     */
    public RedisValue<String> ping(CacheKey cacheKey);

    // expiration
    /**
     * Expire.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param seconds the seconds
     * @return the redis value
     */
    public RedisValue<Long> expire(CacheKey cacheKey, String key, int seconds);

    // sorted sets
    /**
     * Zadd.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param score the score
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Long> zadd(CacheKey cacheKey, String key, double score, String member);

    /**
     * Zincrby.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param score the score
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Double> zincrby(CacheKey cacheKey, String key, double score, String member);

    /**
     * Zscore.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Double> zscore(CacheKey cacheKey, String key, String member);

    /**
     * Zrank.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Long> zrank(CacheKey cacheKey, String key, String member);

    /**
     * Zcount.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the redis value
     */
    public RedisValue<Long> zcount(CacheKey cacheKey, String key, double min, double max);

    /**
     * Zrevrank.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Long> zrevrank(CacheKey cacheKey, String key, String member);

    /**
     * Zrem.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Long> zrem(CacheKey cacheKey, String key, String member);

    /**
     * Zremrange by rank.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param start the start
     * @param end the end
     * @return the redis value
     */
    public RedisValue<Long> zremrangeByRank(CacheKey cacheKey, String key, int start, int end);

    /**
     * Zcard.
     *
     * @param cacheKey the cache key
     * @param string the string
     * @return the redis value
     */
    public RedisValue<Long> zcard(CacheKey cacheKey, String string);

    /**
     * Zrevrange.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param start the start
     * @param end the end
     * @return the redis value
     */
    public RedisValue<Set<String>> zrevrange(CacheKey cacheKey, String key, int start, int end);

    /**
     * Zrevrange by score.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param min the min
     * @param max the max
     * @param offset the offset
     * @param count the count
     * @return the redis value
     */
    public RedisValue<Set<String>> zrevrangeByScore(CacheKey cacheKey, String key, double min, double max, int offset, int count);

    /**
     * Zrevrange by score.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the redis value
     */
    public RedisValue<Set<String>> zrevrangeByScore(CacheKey cacheKey, String key, double min, double max);

    /**
     * Zrange.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param start the start
     * @param end the end
     * @return the redis value
     */
    public RedisValue<Set<String>> zrange(CacheKey cacheKey, String key, int start, int end);

    /**
     * Zrange with scores.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param start the start
     * @param end the end
     * @return the redis value
     */
    public RedisValue<Set<Tuple>> zrangeWithScores(CacheKey cacheKey, String key, int start, int end);

    /**
     * Zrange by score.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param min the min
     * @param max the max
     * @param offset the offset
     * @param count the count
     * @return the redis value
     */
    public RedisValue<Set<String>> zrangeByScore(CacheKey cacheKey, String key, double min, double max, int offset, int count);

    /**
     * Zrange by score with scores.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param min the min
     * @param max the max
     * @param offset the offset
     * @param count the count
     * @return the redis value
     */
    public RedisValue<Set<Tuple>> zrangeByScoreWithScores(CacheKey cacheKey, String key, double min, double max, int offset, int count);

    /**
     * Zrevrange by score with scores.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param max the max
     * @param min the min
     * @param offset the offset
     * @param count the count
     * @return the redis value
     */
    public RedisValue<Set<Tuple>> zrevrangeByScoreWithScores(CacheKey cacheKey, String key, double max, double min, int offset, int count);

    /**
     * Zrange by score.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param min the min
     * @param max the max
     * @return the redis value
     */
    public RedisValue<Set<String>> zrangeByScore(CacheKey cacheKey, String key, double min, double max);

    /**
     * Zinterstore.
     *
     * @param cacheKey the cache key
     * @param dstkey the dstkey
     * @param params the params
     * @param sets the sets
     * @return the redis value
     */
    public RedisValue<Long> zinterstore(CacheKey cacheKey, String dstkey, ZParams params, String... sets);

    /**
     * Mget.
     *
     * @param cacheKey the cache key
     * @param keys the keys
     * @return the redis value
     */
    public RedisValue<List<String>> mget(CacheKey cacheKey, String... keys);

    // lists
    /**
     * Rpush.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param values the values
     * @return the redis value
     */
    public RedisValue<Long> rpush(CacheKey cacheKey, String key, String... values);

    /**
     * Rpoplpush.
     *
     * @param cacheKey the cache key
     * @param srckey the srckey
     * @param dstkey the dstkey
     * @return the redis value
     */
    public RedisValue<String> rpoplpush(CacheKey cacheKey, String srckey, String dstkey);

    /**
     * Lpush.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param values the values
     * @return the redis value
     */
    public RedisValue<Long> lpush(CacheKey cacheKey, String key, String... values);

    /**
     * Brpop.
     *
     * @param cacheKey the cache key
     * @param secondsBlock the seconds block
     * @param key the key
     * @return the redis value
     */
    public RedisValue<List<String>> brpop(CacheKey cacheKey, int secondsBlock, String key);

    /**
     * Lpop.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<String> lpop(CacheKey cacheKey, String key);

    /**
     * Blpop.
     *
     * @param cacheKey the cache key
     * @param secondsBlock the seconds block
     * @param key the key
     * @return the redis value
     */
    public RedisValue<List<String>> blpop(CacheKey cacheKey, int secondsBlock, String key);

    /**
     * Lset.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param index the index
     * @param value the value
     * @return the redis value
     */
    public RedisValue<String> lset(CacheKey cacheKey, String key, int index, String value);

    /**
     * Lrange.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param start the start
     * @param end the end
     * @return the redis value
     */
    public RedisValue<List<String>> lrange(CacheKey cacheKey, String key, int start, int end);

    /**
     * Lindex.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param index the index
     * @return the redis value
     */
    public RedisValue<String> lindex(CacheKey cacheKey, String key, int index);

    /**
     * Lrem.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param count the count
     * @param value the value
     * @return the redis value
     */
    public RedisValue<Long> lrem(CacheKey cacheKey, String key, int count, String value);

    /**
     * Llen.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Long> llen(CacheKey cacheKey, String key);

    /**
     * Ltrim.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param start the start
     * @param end the end
     * @return the redis value
     */
    public RedisValue<String> ltrim(CacheKey cacheKey, String key, int start, int end);

    // sets
    /**
     * Sadd.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param members the members
     * @return the redis value
     */
    public RedisValue<Long> sadd(CacheKey cacheKey, String key, String... members);

    /**
     * Srem.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param members the members
     * @return the redis value
     */
    public RedisValue<Long> srem(CacheKey cacheKey, String key, String... members);

    /**
     * Sismember.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param member the member
     * @return the redis value
     */
    public RedisValue<Boolean> sismember(CacheKey cacheKey, String key, String member);

    /**
     * Smembers.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Set<String>> smembers(CacheKey cacheKey, String key);

    /**
     * Scard.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @return the redis value
     */
    public RedisValue<Long> scard(CacheKey cacheKey, String key);

    /**
     * Sinterstore.
     *
     * @param cacheKey the cache key
     * @param dstkey the dstkey
     * @param sets the sets
     * @return the redis value
     */
    public RedisValue<Long> sinterstore(CacheKey cacheKey, String dstkey, String... sets);

    /**
     * Sinter.
     *
     * @param cacheKey the cache key
     * @param sets the sets
     * @return the redis value
     */
    public RedisValue<Set<String>> sinter(CacheKey cacheKey, String... sets);

    /**
     * Sunionstore.
     *
     * @param cacheKey the cache key
     * @param dstkey the dstkey
     * @param sets the sets
     * @return the redis value
     */
    public RedisValue<Long> sunionstore(CacheKey cacheKey, String dstkey, String... sets);

    /**
     * Sunion.
     *
     * @param cacheKey the cache key
     * @param sets the sets
     * @return the redis value
     */
    public RedisValue<Set<String>> sunion(CacheKey cacheKey, String... sets);

    // sort
    /**
     * Sort.
     *
     * @param cacheKey the cache key
     * @param key the key
     * @param sortingParameters the sorting parameters
     * @return the redis value
     */
    public RedisValue<List<String>> sort(CacheKey cacheKey, String key, SortingParams sortingParameters);

    // scripts
    /**
     * Evalsha.
     *
     * @param cacheKey the cache key
     * @param sha1 the sha1
     * @param keys the keys
     * @param args the args
     * @return the redis value
     */
    public RedisValue<Object> evalsha(CacheKey cacheKey, String sha1, List<String> keys, List<String> args);



}
