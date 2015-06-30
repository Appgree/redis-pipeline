package com.appgree.core.redis.domain;



/**
 * This Class is used to be passed down among nested RedisProviders so it can be extended to store any new information needed.
 */
public class CacheKey {

    private static long DEFAULT_CACHE_TIME = 200; // ms

    /**
     * Modify the default cache time out for all new cache keys, default value is 200ms
     * 
     * @param defaultTimeout the number of milliseconds the key will be alive in the local cache
     */
    public static void setDefaulCacheTime(long defaultTimeout) {
        DEFAULT_CACHE_TIME = defaultTimeout;
    }

    private String name;
    private boolean canCacheReads;
    private long cacheTtl;
    private boolean forceRefresh = false;

    /**
     * Constructor for a CacheKey that is NOT a counter that must be aggregated from all servers
     *
     * @param name
     * @param canCacheReads
     */
    public CacheKey(String name, boolean canCacheReads) {
        this.name = name;
        this.canCacheReads = canCacheReads;
        this.cacheTtl = DEFAULT_CACHE_TIME;
    }

    /**
     * TTL Constructor for a CacheKey that is NOT a counter that must be aggregated from all servers
     *
     * @param name
     * @param canCacheReads
     * @param canBeClusterized
     * @param writeToAllServers
     * @param cacheTtl
     */
    public CacheKey(String name, boolean canCacheReads, long cacheTtl) {
        this.name = name;
        this.canCacheReads = canCacheReads;
        this.cacheTtl = cacheTtl;
    }

    /**
     * Private constructuor ot create a clone of the CacheKey with some values overwritten
     * 
     * @param name
     * @param canCacheReads
     * @param cacheTtl
     * @param forceRefresh
     */
    private CacheKey(String name, boolean canCacheReads, long cacheTtl, boolean forceRefresh) {
        this.name = name;
        this.canCacheReads = canCacheReads;
        this.cacheTtl = cacheTtl;
        this.forceRefresh = forceRefresh;
    }

    public CacheKey getInstance(boolean forceRefresh) {
        return new CacheKey(this.name, this.canCacheReads, this.cacheTtl, forceRefresh);
    }


    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the canCacheReads
     */
    public boolean canCacheReads() {
        return canCacheReads;
    }

    /**
     * @return the cacheTtl
     */
    public long getCacheTtl() {
        return cacheTtl;
    }

    /**
     * @return the forceRefresh
     */
    public boolean isForceRefresh() {
        return forceRefresh;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (canCacheReads ? 1231 : 1237);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CacheKey other = (CacheKey) obj;
        if (canCacheReads != other.canCacheReads)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getName();
    }

    /**
     * Method employed to get a String with the key's settings.
     * 
     * @return A String containing a human readable representation of the CacheKey
     */
    public String getDebugInfo() {
        StringBuffer sb = new StringBuffer(this.name);
        sb.append(",");
        for (int i = 0; i < (20 - this.name.length()); i++) {
            sb.append(' ');
        }
        sb.append(",\tCache reads: ");
        sb.append(this.canCacheReads);
        sb.append(",\tCache timeout: ");
        sb.append(this.cacheTtl);
        return sb.toString();
    }

}
