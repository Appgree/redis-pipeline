package com.appgree.core.redis.helper;

/**
 * The Class RedisHelper.
 */
public class RedisHelper {

    /**
     * Parses a jedis integer value, if input value is 'nil' returns 0 (as in JDBC - MySQL).
     *
     * @param value the string to parse containing an int
     * @return 0 if said string is 'nil' or the parsed int value
     */
    public static int parseInt(String value) {
        if (value == null || value.equals("") || value.equals("nil")) {
            return 0;
        } else {
            return Integer.parseInt(value);
        }
    }

}
