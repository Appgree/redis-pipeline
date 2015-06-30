package com.appgree.core.redis.provider;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import com.appgree.core.redis.domain.CacheKey;

/**
 * The Class RedisRemoteProvider.
 */
public class RedisRemoteProvider extends RedisRemoteBaseProvider implements RedisProvider, RedisSentinelNotifiable {

    /** The logger. */
    private static Logger logger = Logger.getLogger(RedisRemoteProvider.class.getName());

    /** The redis error retry. */
    private long redisErrorRetry;

    /**
     * Instantiates a new redis remote provider.
     *
     * @param redisErrorRetry the redis error retry
     * @throws Exception the exception
     */
    public RedisRemoteProvider(long redisErrorRetry) throws Exception {
        this.redisErrorRetry = redisErrorRetry;
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#set(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> set(CacheKey cacheKey, String key, String value) {
        if (DEBUG_REDIS_CACHE) {
            logCall("set", key, value);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.set(key, value));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#get(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> get(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("get", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.get(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#getSet(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> getSet(CacheKey cacheKey, String key, String value) {
        if (DEBUG_REDIS_CACHE) {
            logCall("getSet", key, value);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.getSet(key, value));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hkeys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> hkeys(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hkeys", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.hkeys(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hvals(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> hvals(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hvals", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<List<String>>()).setValue(jedis.hvals(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hset(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hset(CacheKey cacheKey, String key, String field, String value) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hset", key, value);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.hset(key, field, value));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hdel(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> hdel(CacheKey cacheKey, String key, String field) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hdel", key, field);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.hdel(key, field));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmset(dw.cache.provider.CacheKey, java.lang.String, java.util.Map)
     */
    @Override
    public RedisValue<String> hmset(CacheKey cacheKey, String key, Map<String, String> hash) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hmset", key, hash.toString());
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.hmset(key, hash));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> hget(CacheKey cacheKey, String key, String field) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hget", key, field);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.hget(key, field));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hmget(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<List<String>> hmget(CacheKey cacheKey, String key, String... fields) {
        if (DEBUG_REDIS_CACHE) {
            String[] fieldList = new String[fields.length + 1];
            fieldList[0] = key;
            for (int i = 0; i < fields.length; i++) {
                fieldList[i + 1] = fields[i];
            }
            logCall("hmget", fieldList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<List<String>>()).setValue(jedis.hmget(key, fields));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hgetAll(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Map<String, String>> hgetAll(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hgetAll", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Map<String, String>>()).setValue(jedis.hgetAll(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hincrBy(dw.cache.provider.CacheKey, java.lang.String, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> hincrBy(CacheKey cacheKey, String key, String field, int value) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hincrBy", key, field, String.valueOf(value));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.hincrBy(key, field, value));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hexists(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> hexists(CacheKey cacheKey, String key, String field) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hexists", key, field);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Boolean>()).setValue(jedis.hexists(key, field));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#hlen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> hlen(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("hlen", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.hlen(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ping(dw.cache.provider.CacheKey)
     */
    @Override
    public RedisValue<String> ping(CacheKey cacheKey) {
        if (DEBUG_REDIS_CACHE) {
            logCall("ping");
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.ping());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#expire(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<Long> expire(CacheKey cacheKey, String key, int seconds) {
        if (DEBUG_REDIS_CACHE) {
            logCall("expire", key, String.valueOf(seconds));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.expire(key, seconds));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#keys(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> keys(CacheKey cacheKey, String pattern) {
        if (DEBUG_REDIS_CACHE) {
            logCall("keys", pattern);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.keys(pattern));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#del(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Long> del(CacheKey cacheKey, String... keys) {
        if (DEBUG_REDIS_CACHE) {
            logCall("del", keys);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.del(keys));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#exists(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> exists(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("exists", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Boolean>()).setValue(jedis.exists(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#incr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> incr(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("incr", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.incr(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#decr(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> decr(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("decr", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.decr(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zadd(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Long> zadd(CacheKey cacheKey, String key, double score, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zadd", key, String.valueOf(score), member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zadd(key, score, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zincrby(dw.cache.provider.CacheKey, java.lang.String, double, java.lang.String)
     */
    @Override
    public RedisValue<Double> zincrby(CacheKey cacheKey, String key, double score, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zincrby", key, String.valueOf(score), member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Double>()).setValue(jedis.zincrby(key, score, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zscore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Double> zscore(CacheKey cacheKey, String key, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zscore", key, member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Double>()).setValue(jedis.zscore(key, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrank(CacheKey cacheKey, String key, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrank", key, member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zrank(key, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcount(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Long> zcount(CacheKey cacheKey, String key, double min, double max) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zcount", key, String.valueOf(min), String.valueOf(max));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zcount(key, min, max));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrank(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrevrank(CacheKey cacheKey, String key, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrevrank", key, member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zrevrank(key, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Long> zrem(CacheKey cacheKey, String key, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrem", key, member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zrem(key, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zremrangeByRank(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Long> zremrangeByRank(CacheKey cacheKey, String key, int start, int end) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zremrangeByRank", key, String.valueOf(start), String.valueOf(end));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zremrangeByRank(key, start, end));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zcard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> zcard(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zcard", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zcard(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrevrange(CacheKey cacheKey, String key, int start, int end) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrevrange", key, String.valueOf(start), String.valueOf(end));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.zrevrange(key, start, end));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(CacheKey cacheKey, String key, double min, double max, int offset, int count) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrevrangeByScore", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.zrevrangeByScore(key, min, max, offset, count));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Set<String>> zrevrangeByScore(CacheKey cacheKey, String key, double min, double max) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrevrangeByScore", key, String.valueOf(min), String.valueOf(max));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.zrevrangeByScore(key, min, max));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrange(CacheKey cacheKey, String key, int start, int end) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrange", key, String.valueOf(start), String.valueOf(end));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.zrange(key, start, end));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeWithScores(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrangeWithScores(CacheKey cacheKey, String key, int start, int end) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrangeWithScores", key, String.valueOf(start), String.valueOf(end));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<Tuple>>()).setValue(jedis.zrangeWithScores(key, start, end));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<String>> zrangeByScore(CacheKey cacheKey, String key, double min, double max, int offset, int count) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrangeByScore", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.zrangeByScore(key, min, max, offset, count));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrangeByScoreWithScores(CacheKey cacheKey, String key, double min, double max, int offset, int count) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrangeByScoreWithScores", key, String.valueOf(min), String.valueOf(max), String.valueOf(offset), String.valueOf(count));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<Tuple>>()).setValue(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrevrangeByScoreWithScores(dw.cache.provider.CacheKey, java.lang.String, double, double, int, int)
     */
    @Override
    public RedisValue<Set<Tuple>> zrevrangeByScoreWithScores(CacheKey cacheKey, String key, double max, double min, int offset, int count) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrevrangeByScoreWithScores", key, String.valueOf(max), String.valueOf(min), String.valueOf(offset), String.valueOf(count));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<Tuple>>()).setValue(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zrangeByScore(dw.cache.provider.CacheKey, java.lang.String, double, double)
     */
    @Override
    public RedisValue<Set<String>> zrangeByScore(CacheKey cacheKey, String key, double min, double max) {
        if (DEBUG_REDIS_CACHE) {
            logCall("zrangeByScore", key, String.valueOf(min), String.valueOf(max));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.zrangeByScore(key, min, max));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#zinterstore(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.ZParams,
     * java.lang.String[])
     */
    @Override
    public RedisValue<Long> zinterstore(CacheKey cacheKey, String dstkey, ZParams params, String... sets) {
        if (DEBUG_REDIS_CACHE) {
            String[] setList = new String[sets.length + 1];
            setList[0] = dstkey;
            for (int i = 0; i < sets.length; i++) {
                setList[i + 1] = sets[i];
            }
            logCall("zinterstore", setList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.zinterstore(dstkey, params, sets));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#mget(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<List<String>> mget(CacheKey cacheKey, String... keys) {
        if (DEBUG_REDIS_CACHE) {
            logCall("mget", keys);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<List<String>>()).setValue(jedis.mget(keys));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> rpush(CacheKey cacheKey, String key, String... values) {
        if (DEBUG_REDIS_CACHE) {
            String[] valueList = new String[values.length + 1];
            valueList[0] = key;
            for (int i = 0; i < values.length; i++) {
                valueList[i + 1] = values[i];
            }
            logCall("rpush", valueList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.rpush(key, values));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#rpoplpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<String> rpoplpush(CacheKey cacheKey, String srckey, String dstkey) {
        if (DEBUG_REDIS_CACHE) {
            logCall("rpoplpush", srckey, dstkey);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.rpoplpush(srckey, dstkey));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpush(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> lpush(CacheKey cacheKey, String key, String... values) {
        if (DEBUG_REDIS_CACHE) {
            String[] valueList = new String[values.length + 1];
            valueList[0] = key;
            for (int i = 0; i < values.length; i++) {
                valueList[i + 1] = values[i];
            }
            logCall("lpush", valueList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.lpush(key, values));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }


    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lpop(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<String> lpop(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("lpop", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.lpop(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#brpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> brpop(CacheKey cacheKey, int secondsBlock, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("brpop", String.valueOf(secondsBlock), key);
        }
        Jedis jedis = pool.getResource();
        try {
            RedisValue<List<String>> res = (new RedisValue<List<String>>()).setValue(jedis.brpop(secondsBlock, key));
            return res;
        } catch (Throwable e) {
            logger.error("Error executing brpop. Waiting " + redisErrorRetry + "ms", e);
            try {
                Thread.sleep(redisErrorRetry);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }

            throw new RuntimeException("Error executing brpop", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#blpop(dw.cache.provider.CacheKey, int, java.lang.String)
     */
    @Override
    public RedisValue<List<String>> blpop(CacheKey cacheKey, int secondsBlock, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("blpop", String.valueOf(secondsBlock), key);
        }
        Jedis jedis = pool.getResource();
        try {
            RedisValue<List<String>> res = (new RedisValue<List<String>>()).setValue(jedis.blpop(secondsBlock, key));
            return res;
        } catch (Throwable e) {
            logger.error("Error executing blpop. Waiting " + redisErrorRetry + "ms", e);
            try {
                Thread.sleep(redisErrorRetry);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }

            throw new RuntimeException("Error executing blpop", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lset(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<String> lset(CacheKey cacheKey, String key, int index, String value) {
        if (DEBUG_REDIS_CACHE) {
            logCall("lset", key, String.valueOf(index), value);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.lset(key, index, value));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrange(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<List<String>> lrange(CacheKey cacheKey, String key, int start, int end) {
        if (DEBUG_REDIS_CACHE) {
            logCall("lset", key, String.valueOf(start), String.valueOf(end));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<List<String>>()).setValue(jedis.lrange(key, start, end));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lindex(dw.cache.provider.CacheKey, java.lang.String, int)
     */
    @Override
    public RedisValue<String> lindex(CacheKey cacheKey, String key, int index) {
        if (DEBUG_REDIS_CACHE) {
            logCall("lindex", key, String.valueOf(index));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.lindex(key, index));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#lrem(dw.cache.provider.CacheKey, java.lang.String, int, java.lang.String)
     */
    @Override
    public RedisValue<Long> lrem(CacheKey cacheKey, String key, int count, String value) {
        if (DEBUG_REDIS_CACHE) {
            logCall("lrem", key, String.valueOf(count), value);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.lrem(key, count, value));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#ltrim(dw.cache.provider.CacheKey, java.lang.String, int, int)
     */
    @Override
    public RedisValue<String> ltrim(CacheKey cacheKey, String key, int start, int end) {
        if (DEBUG_REDIS_CACHE) {
            logCall("ltrim", key, String.valueOf(start), String.valueOf(end));
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<String>()).setValue(jedis.ltrim(key, start, end));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#llen(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> llen(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("llen", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.llen(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sadd(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sadd(CacheKey cacheKey, String key, String... members) {
        if (DEBUG_REDIS_CACHE) {
            String[] memberList = new String[members.length + 1];
            memberList[0] = key;
            for (int i = 0; i < members.length; i++) {
                memberList[i + 1] = members[i];
            }
            logCall("sadd", memberList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.sadd(key, members));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#srem(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> srem(CacheKey cacheKey, String key, String... members) {
        if (DEBUG_REDIS_CACHE) {
            String[] memberList = new String[members.length + 1];
            memberList[0] = key;
            for (int i = 0; i < members.length; i++) {
                memberList[i + 1] = members[i];
            }
            logCall("srem", memberList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.srem(key, members));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sismember(dw.cache.provider.CacheKey, java.lang.String, java.lang.String)
     */
    @Override
    public RedisValue<Boolean> sismember(CacheKey cacheKey, String key, String member) {
        if (DEBUG_REDIS_CACHE) {
            logCall("sismember", key, member);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Boolean>()).setValue(jedis.sismember(key, member));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#smembers(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Set<String>> smembers(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("smembers", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.smembers(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#scard(dw.cache.provider.CacheKey, java.lang.String)
     */
    @Override
    public RedisValue<Long> scard(CacheKey cacheKey, String key) {
        if (DEBUG_REDIS_CACHE) {
            logCall("scard", key);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.scard(key));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinterstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sinterstore(CacheKey cacheKey, String dstkey, String... sets) {
        if (DEBUG_REDIS_CACHE) {
            String[] setList = new String[sets.length + 1];
            setList[0] = dstkey;
            for (int i = 0; i < sets.length; i++) {
                setList[i + 1] = sets[i];
            }
            logCall("sinterstore", setList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.sinterstore(dstkey, sets));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sinter(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Set<String>> sinter(CacheKey cacheKey, String... sets) {
        if (DEBUG_REDIS_CACHE) {
            logCall("sinter", sets);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.sinter(sets));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunionstore(dw.cache.provider.CacheKey, java.lang.String, java.lang.String[])
     */
    @Override
    public RedisValue<Long> sunionstore(CacheKey cacheKey, String dstkey, String... sets) {
        if (DEBUG_REDIS_CACHE) {
            String[] setList = new String[sets.length + 1];
            setList[0] = dstkey;
            for (int i = 0; i < sets.length; i++) {
                setList[i + 1] = sets[i];
            }
            logCall("sunionstore", setList);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Long>()).setValue(jedis.sunionstore(dstkey, sets));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sunion(dw.cache.provider.CacheKey, java.lang.String[])
     */
    @Override
    public RedisValue<Set<String>> sunion(CacheKey cacheKey, String... sets) {
        if (DEBUG_REDIS_CACHE) {
            logCall("sunion", sets);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Set<String>>()).setValue(jedis.sunion(sets));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#sort(dw.cache.provider.CacheKey, java.lang.String, redis.clients.jedis.SortingParams)
     */
    @Override
    public RedisValue<List<String>> sort(CacheKey cacheKey, String key, SortingParams sortingParameters) {
        if (DEBUG_REDIS_CACHE) {
            logCall("sort", key, sortingParameters.toString());
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<List<String>>()).setValue(jedis.sort(key, sortingParameters));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see dw.core.redis.provider.RedisProvider#evalsha(dw.cache.provider.CacheKey, java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public RedisValue<Object> evalsha(CacheKey cacheKey, String sha1, List<String> keys, List<String> args) {
        if (DEBUG_REDIS_CACHE) {
            logCall("evalsha", sha1);
        }
        Jedis jedis = pool.getResource();
        try {
            return (new RedisValue<Object>()).setValue(jedis.evalsha(sha1, keys, args));
        } finally {
            if (jedis != null) {
                jedis.close();
            };
        }
    }

    /**
     * Log call.
     *
     * @param methodId the method id
     * @param params the params
     */
    private void logCall(String methodId, String... params) {
        String paramString = concat(methodId, params);
        logger.info("REAL     : " + paramString);
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
        return builder.toString();
    }

}
