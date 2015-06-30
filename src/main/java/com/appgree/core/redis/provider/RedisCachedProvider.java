package com.appgree.core.redis.provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import com.appgree.core.redis.domain.CacheKey;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * The high level Redis cache manager This class parameterized cached elements using an external provider.
 */
public class RedisCachedProvider implements RedisProvider {

    // the logger object
    /** The logger. */
    private static Logger logger = Logger.getLogger(RedisCachedProvider.class.getName());

    /**
     * This class wrapps the low level cache object.
     */
    private class CacheWrapper {

        // the internal cache
        /** The internal cache. */
        private Cache<String, RedisValue<?>> internalCache;

        /**
         * The main constructor.
         *
         * @param cache The internal cache
         */
        public CacheWrapper(Cache<String, RedisValue<?>> cache) {
            this.internalCache = cache;
        }

        /**
         * The getter.
         *
         * @param key The cache key object
         * @param keyValue The element key
         * @param loader The element loader
         * @return Returns the wrapped return value
         */
        public RedisValue<?> get(CacheKey key, String keyValue, Callable<RedisValue<?>> loader) {

            try {

                // checks cacheable status
                if (key.canCacheReads() == false) {

                    // the element is not cacheable. We have to skip the cache layer
                    return loader.call();
                }

                // checks TTL
                boolean forceRefresh = checkTTL(key, keyValue);

                // checks if we have to refresh the object in the cache
                if (forceRefresh || key.isForceRefresh()) {
                    internalCache.invalidate(keyValue);
                }

                // proceed with the standard caching procedure
                return internalCache.get(keyValue, loader);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Checks TTL based expiration.
         *
         * @param key the key
         * @param keyValue the key value
         * @return True if TTL was reached
         */
        private boolean checkTTL(CacheKey key, String keyValue) {
            long ttl = key.getCacheTtl();

            if (ttl == 0) {
                return false;
            }

            // retrieves the cached value
            RedisValue<?> value = (RedisValue<?>) internalCache.getIfPresent(keyValue);
            if (value == null || value.getTtlReference() == 0) {
                return false;
            }

            // checks time expiration
            return (System.currentTimeMillis() - value.getTtlReference()) > key.getCacheTtl();
        }
    }

    // the cache entity
    /** The cache wrapper. */
    private CacheWrapper cacheWrapper;

    // the provider
    /** The provider. */
    private RedisProvider provider;

    // Private constructor prevents instantiation from other classes
    /**
     * Instantiates a new REDIS cached provider.
     *
     * @param provider the provider
     * @param timeout the timeout
     * @param maxElements the max elements
     */
    public RedisCachedProvider(RedisProvider provider, long timeout, long maxElements) {
        this.provider = provider;
        Cache<String, RedisValue<?>> newCache = CacheBuilder.newBuilder().maximumSize(maxElements).expireAfterWrite(timeout, TimeUnit.MILLISECONDS)
                        .build();

        cacheWrapper = new CacheWrapper(newCache);
    }

    /**
     * Generates a string concat.
     *
     * @param methodId The method id
     * @param fields The method keys
     * @return The concat
     */
    private static String concat(String methodId, String... fields) {
        StringBuilder builder = new StringBuilder();
        builder.append(methodId);
        for (String item : fields) {
            builder.append("[");
            builder.append(item == null ? "nil" : item);
        }
        if (DEBUG_REDIS_CACHE) {
            logger.info("CACHE    : " + builder.toString());
        }
        return builder.toString();
    }

    /**
     * Flush local cache.
     */
    public void flushLocalCache() {
        this.cacheWrapper.internalCache.invalidateAll();
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

        return provider.set(cacheKey, key, value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#get(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<String> get(final CacheKey cacheKey, final String key) {

        final String localKey = concat("get", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.get(cacheKey, key);
            }
        });

        return (RedisValue<String>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#getSet(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<String> getSet(final CacheKey cacheKey, final String key, final String value) {

        final String localKey = concat("getSet", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.getSet(cacheKey, key, value);
            }
        });

        return (RedisValue<String>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hset(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hset(final CacheKey cacheKey, final String key, final String field, final String value) {

        return provider.hset(cacheKey, key, field, value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hdel(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hdel(final CacheKey cacheKey, final String key, final String field) {

        return provider.hdel(cacheKey, key, field);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmset(dw.cache.provider.CacheKey, java.lang.String, java.util.Map)
     */
    @Override
    public RedisValue<String> hmset(final CacheKey cacheKey, final String key, final Map<String, String> hash) {

        return provider.hmset(cacheKey, key, hash);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<String> hget(final CacheKey cacheKey, final String key, final String field) {

        final String localKey = concat("hget", key, field);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hget(cacheKey, key, field);
            }
        });

        return (RedisValue<String>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<List<String>> hmget(final CacheKey cacheKey, final String key, final String... fields) {

        String[] field = new String[fields.length + 1];
        field[0] = key;
        for (int i = 0; i < fields.length; i++) {
            field[i + 1] = fields[i];
        }
        final String localKey = concat("hmget", field);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hmget(cacheKey, key, fields);
            }
        });

        return (RedisValue<List<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hgetAll(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Map<String, String>> hgetAll(final CacheKey cacheKey, final String key) {
        final String localKey = concat("hgetAll", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hgetAll(cacheKey, key);
            }
        });

        return (RedisValue<Map<String, String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hexists(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Boolean> hexists(final CacheKey cacheKey, final String key, final String field) {
        final String localKey = concat("hexists", key, field);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hexists(cacheKey, key, field);
            }
        });

        return (RedisValue<Boolean>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hlen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Long> hlen(final CacheKey cacheKey, final String key) {
        final String localKey = concat("hlen", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hlen(cacheKey, key);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hincrBy(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> hincrBy(final CacheKey cacheKey, final String key, final String field, final int value) {
        return provider.hincrBy(cacheKey, key, field, value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ping(dw.cache.provider.CacheKey)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<String> ping(final CacheKey cacheKey) {
        final String localKey = concat("ping");

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.ping(cacheKey);
            }
        });

        return (RedisValue<String>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#expire(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> expire(final CacheKey cacheKey, final String key, final int seconds) {
        return provider.expire(cacheKey, key, seconds);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#del(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Long> del(final CacheKey cacheKey, final String... keys) {
        return provider.del(cacheKey, keys);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#exists(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Boolean> exists(final CacheKey cacheKey, final String key) {
        final String localKey = concat("exists", key);
        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.exists(cacheKey, key);
            }
        });

        return (RedisValue<Boolean>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#incr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> incr(final CacheKey cacheKey, final String key) {
        return provider.incr(cacheKey, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#decr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> decr(final CacheKey cacheKey, final String key) {
        return provider.decr(cacheKey, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#keys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    @Deprecated
    public RedisValue<Set<String>> keys(final CacheKey cacheKey, final String pattern) {
        final String localKey = concat("keys", pattern);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.keys(cacheKey, pattern);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zadd(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Long> zadd(final CacheKey cacheKey, final String key, final double score, final String member) {
        if (DEBUG_REDIS_CACHE) {
            concat("zadd", key, String.valueOf(score), member);
        }
        return provider.zadd(cacheKey, key, score, member);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zincrby(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Double> zincrby(final CacheKey cacheKey, final String key, final double score, final String member) {
        if (DEBUG_REDIS_CACHE) {
            concat("zincrBy", key, String.valueOf(score), member);
        }
        return provider.zincrby(cacheKey, key, score, member);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zscore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Double> zscore(final CacheKey cacheKey, final String key, final String member) {
        if (DEBUG_REDIS_CACHE) {
            concat("zscore", key, member);
        }
        return provider.zscore(cacheKey, key, member);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrank(final CacheKey cacheKey, final String key, final String member) {
        if (DEBUG_REDIS_CACHE) {
            concat("zrank", key, member);
        }
        return provider.zrank(cacheKey, key, member);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcount(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Long> zcount(final CacheKey cacheKey, final String key, final double min, final double max) {
        if (DEBUG_REDIS_CACHE) {
            concat("zrank", key, String.valueOf(min), String.valueOf(max));
        }
        return provider.zcount(cacheKey, key, min, max);
    }


    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrevrank(final CacheKey cacheKey, final String key, final String member) {
        if (DEBUG_REDIS_CACHE) {
            concat("zrevrank", key, member);
        }
        return provider.zrevrank(cacheKey, key, member);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrem(final CacheKey cacheKey, final String key, final String member) {
        if (DEBUG_REDIS_CACHE) {
            concat("zrem", key, member);
        }
        return provider.zrem(cacheKey, key, member);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zremrangeByRank(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Long> zremrangeByRank(final CacheKey cacheKey, final String key, final int start, final int end) {
        if (DEBUG_REDIS_CACHE) {
            concat("zremrangeByRank", key, String.valueOf(start), String.valueOf(end));
        }
        return provider.zremrangeByRank(cacheKey, key, start, end);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Long> zcard(final CacheKey cacheKey, final String key) {
        final String localKey = concat("zcard", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zcard(cacheKey, key);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> zrevrange(final CacheKey cacheKey, final String key, final int start, final int end) {
        final String localKey = concat("zrevrange", key, String.valueOf(start), String.valueOf(end));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrevrange(cacheKey, key, start, end);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max, final int offset,
                    final int count) {
        final String localKey = concat("zrevrangeByScore", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset),
                        String.valueOf(count));
        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrevrangeByScore(cacheKey, key, min, max, offset, count);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max) {
        final String localKey = concat("zrevrangeByScore", key, String.valueOf(min), String.valueOf(max));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrevrangeByScore(cacheKey, key, min, max);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> zrange(final CacheKey cacheKey, final String key, final int start, final int end) {
        final String localKey = concat("zrange", key, String.valueOf(start), String.valueOf(end));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrange(cacheKey, key, start, end);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeWithScores(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<Tuple>> zrangeWithScores(final CacheKey cacheKey, final String key, final int start, final int end) {
        final String localKey = concat("zrangewscores", key, String.valueOf(start), String.valueOf(end));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrangeWithScores(cacheKey, key, start, end);
            }
        });

        return (RedisValue<Set<Tuple>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> zrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max, final int offset,
                    final int count) {
        final String localKey = concat("zrangeByScore", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrangeByScore(cacheKey, key, min, max, offset, count);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<Tuple>> zrangeByScoreWithScores(final CacheKey cacheKey, final String key, final double min, final double max,
                    final int offset, final int count) {
        final String localKey = concat("zrangeByScorewscores", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset),
                        String.valueOf(count));
        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrangeByScoreWithScores(cacheKey, key, min, max, offset, count);
            }
        });

        return (RedisValue<Set<Tuple>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<Tuple>> zrevrangeByScoreWithScores(final CacheKey cacheKey, final String key, final double max, final double min,
                    final int offset, final int count) {
        final String localKey = concat("zrevrangeByScorewscores", key, String.valueOf(max), String.valueOf(min), String.valueOf(offset),
                        String.valueOf(count));
        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrevrangeByScoreWithScores(cacheKey, key, max, min, offset, count);
            }
        });

        return (RedisValue<Set<Tuple>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zinterstore(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.ZParams,
     * java.lang.String[])
     */
    @SuppressWarnings("unchecked")
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
        final String localKey = concat("zinterstore", actualParams);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zinterstore(cacheKey, dstkey, params, sets);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> zrangeByScore(final CacheKey cacheKey, final String key, final double min, final double max) {
        final String localKey = concat("zrangeByScore", key, String.valueOf(min), String.valueOf(max));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.zrangeByScore(cacheKey, key, min, max);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#mget(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<List<String>> mget(final CacheKey cacheKey, final String... keys) {
        final String localKey = concat("mget", keys);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.mget(cacheKey, keys);
            }
        });

        return (RedisValue<List<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> rpush(final CacheKey cacheKey, final String key, final String... values) {
        if (DEBUG_REDIS_CACHE) {
            String[] valueList = new String[values.length + 1];
            valueList[0] = key;
            for (int i = 0; i < values.length; i++) {
                valueList[i + 1] = values[i];
            }
            concat("rpush", valueList);
        }
        return provider.rpush(cacheKey, key, values);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpoplpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> rpoplpush(final CacheKey cacheKey, final String srckey, final String dstkey) {
        return provider.rpoplpush(cacheKey, srckey, dstkey);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> lpush(final CacheKey cacheKey, final String key, final String... values) {
        if (DEBUG_REDIS_CACHE) {
            String[] valueList = new String[values.length + 1];
            valueList[0] = key;
            for (int i = 0; i < values.length; i++) {
                valueList[i + 1] = values[i];
            }
            concat("lpush", valueList);
        }

        return provider.lpush(cacheKey, key, values);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpop(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> lpop(final CacheKey cacheKey, final String key) {
        if (cacheKey.canCacheReads() && !cacheKey.isForceRefresh()) {
            // there is no way to simulate a blocking operation nor caching or pipelining
            throw new UnsupportedOperationException();
        }

        return provider.lpop(cacheKey, key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#brpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> brpop(final CacheKey cacheKey, final int secondsBlock, final String key) {
        if (cacheKey.canCacheReads() && !cacheKey.isForceRefresh()) {
            // there is no way to simulate a blocking operation nor caching or pipelining
            throw new UnsupportedOperationException();
        }
        return provider.brpop(cacheKey, secondsBlock, key).flush();
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#blpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> blpop(final CacheKey cacheKey, final int secondsBlock, final String key) {
        if (cacheKey.canCacheReads() && !cacheKey.isForceRefresh()) {
            // there is no way to simulate a blocking operation nor caching or pipelining
            throw new UnsupportedOperationException();
        }
        return provider.blpop(cacheKey, secondsBlock, key).flush();
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lset(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<String> lset(final CacheKey cacheKey, final String key, final int index, final String value) {
        return provider.lset(cacheKey, key, index, value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<List<String>> lrange(final CacheKey cacheKey, final String key, final int start, final int end) {
        final String localKey = concat("lrange", key, String.valueOf(start), String.valueOf(end));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.lrange(cacheKey, key, start, end);
            }
        });

        return (RedisValue<List<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lindex(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<String> lindex(final CacheKey cacheKey, final String key, final int index) {
        final String localKey = concat("lindex", key, String.valueOf(index));

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.lindex(cacheKey, key, index);
            }
        });

        return (RedisValue<String>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrem(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<Long> lrem(final CacheKey cacheKey, final String key, final int count, final String value) {
        return provider.lrem(cacheKey, key, count, value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ltrim(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<String> ltrim(final CacheKey cacheKey, String key, int start, int end) {
        return provider.ltrim(cacheKey, key, start, end);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#llen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Long> llen(final CacheKey cacheKey, final String key) {
        final String localKey = concat("llen", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.llen(cacheKey, key);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hkeys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> hkeys(final CacheKey cacheKey, final String key) {
        final String localKey = concat("hkeys", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hkeys(cacheKey, key);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hvals(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<List<String>> hvals(final CacheKey cacheKey, final String key) {
        final String localKey = concat("hvals", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.hvals(cacheKey, key);
            }
        });

        return (RedisValue<List<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sadd(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sadd(final CacheKey cacheKey, final String key, final String... members) {
        if (DEBUG_REDIS_CACHE) {
            String[] memberList = new String[members.length + 1];
            memberList[0] = key;
            for (int i = 0; i < members.length; i++) {
                memberList[i + 1] = members[i];
            }
            concat("sadd", memberList);
        }
        return provider.sadd(cacheKey, key, members);
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
            concat("srem", memberList);
        }
        return provider.srem(cacheKey, key, members);
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sismember(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Boolean> sismember(final CacheKey cacheKey, final String key, final String member) {
        final String localKey = concat("sismember", key, member);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.sismember(cacheKey, key, member);
            }
        });

        return (RedisValue<Boolean>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#smembers(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> smembers(final CacheKey cacheKey, final String key) {
        final String localKey = concat("smembers", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.smembers(cacheKey, key);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#scard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Long> scard(final CacheKey cacheKey, final String key) {
        final String localKey = concat("scard", key);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.scard(cacheKey, key);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinterstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Long> sinterstore(final CacheKey cacheKey, final String dstkey, final String... sets) {
        String[] setList = new String[sets.length + 1];
        setList[0] = dstkey;
        for (int i = 0; i < sets.length; i++) {
            setList[i + 1] = sets[i];
        }
        final String localKey = concat("sinterstore", setList);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.sinterstore(cacheKey, dstkey, sets);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinter(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> sinter(final CacheKey cacheKey, final String... sets) {
        final String localKey = concat("sinter", sets);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.sinter(cacheKey, sets);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunionstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Long> sunionstore(final CacheKey cacheKey, final String dstkey, final String... sets) {
        String[] setList = new String[sets.length + 1];
        setList[0] = dstkey;
        for (int i = 0; i < sets.length; i++) {
            setList[i + 1] = sets[i];
        }
        final String localKey = concat("sunionstore", setList);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.sunionstore(cacheKey, dstkey, sets);
            }
        });

        return (RedisValue<Long>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunion(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public RedisValue<Set<String>> sunion(final CacheKey cacheKey, final String... sets) {
        final String localKey = concat("sunion", sets);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.sunion(cacheKey, sets);
            }
        });

        return (RedisValue<Set<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sort(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.SortingParams)
     */
    @Override
    @SuppressWarnings("unchecked")
    public RedisValue<List<String>> sort(final CacheKey cacheKey, final String key, final SortingParams sortingParameters) {
        Collection<byte[]> params = sortingParameters.getParams();
        String[] paramList = new String[params.size() + 1];
        paramList[0] = key;
        int i = 1;
        for (byte[] p : params) {
            try {
                paramList[i++] = new String(p, "UTF-8");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        final String localKey = concat("sort", paramList);

        RedisValue<?> res = cacheWrapper.get(cacheKey, localKey, new Callable<RedisValue<?>>() {

            @Override
            public RedisValue<?> call() throws Exception {
                return provider.sort(cacheKey, key, sortingParameters);
            }
        });

        return (RedisValue<List<String>>) res;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#evalsha(dw.cache.provider.CacheKey, java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public RedisValue<Object> evalsha(final CacheKey cacheKey, final String sha1, final List<String> keys, final List<String> args) {
        return provider.evalsha(cacheKey, sha1, keys, args);
    }
}
