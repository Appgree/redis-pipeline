package com.appgree.core.redis.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

/**
 * The Class RedisSentinelPool.
 */
public class RedisSentinelPool extends Pool<Jedis> {

    /** The logger. */
    private static Logger logger = Logger.getLogger(RedisSentinelPool.class);

    /**
     * The Class HostAndPort.
     */
    public class HostAndPort {

        /** The host. */
        String host;

        /** The port. */
        int port;

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof HostAndPort) {
                final HostAndPort that = (HostAndPort) obj;
                return this.port == that.port && this.host.equals(that.host);
            }
            return false;
        }

        /**
         * Gets the host.
         *
         * @return the host
         */
        public String getHost() {
            return host;
        }

        /**
         * Gets the port.
         *
         * @return the port
         */
        public int getPort() {
            return port;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    /**
     * The Class PoolEntry.
     */
    private class PoolEntry {

        /** The pool. */
        private JedisPool pool;

        /** The master. */
        private HostAndPort master;

        /** The notifiables. */
        private List<RedisSentinelNotifiable> notifiables = new ArrayList<RedisSentinelNotifiable>();

        /**
         * Adds the notifiable.
         *
         * @param notifiable the notifiable
         */
        private void addNotifiable(RedisSentinelNotifiable notifiable) {
            this.notifiables.add(notifiable);
        }

        /**
         * Gets the notifiables.
         *
         * @return the notifiables
         */
        private List<RedisSentinelNotifiable> getNotifiables() {
            return this.notifiables;
        }

        /**
         * Gets the pool.
         *
         * @return the pool
         */
        private JedisPool getPool() {
            return pool;
        }

        /**
         * Gets the master.
         *
         * @return the master
         */
        private HostAndPort getMaster() {
            return master;
        }

        /**
         * Sets the master.
         *
         * @param master the master
         * @param pool the pool
         */
        private void setMaster(HostAndPort master, JedisPool pool) {
            this.pool = pool;
            this.master = master;
        }
    }

    // the master host map
    /** The pool map. */
    private Map<String, PoolEntry> poolMap = new HashMap<String, RedisSentinelPool.PoolEntry>();

    /**
     * A factory for creating jedis objects.
     */
    private static class jedisFactory extends BasePooledObjectFactory<Jedis> {

        @Override
        public PooledObject<Jedis> makeObject() throws Exception {
            return null;
        }

        @Override
        public Jedis create() throws Exception {
            return null;
        }

        @Override
        public PooledObject<Jedis> wrap(Jedis obj) {
            return null;
        }

    }

    /**
     * The Class JedisPubSubAdapter.
     */
    public class JedisPubSubAdapter extends JedisPubSub {

        /*
         * (non-Javadoc)
         * 
         * @see redis.clients.jedis.JedisPubSub#onMessage(java.lang.String, java.lang.String)
         */
        @Override
        public void onMessage(String channel, String message) {

        }

        /*
         * (non-Javadoc)
         * 
         * @see redis.clients.jedis.JedisPubSub#onPMessage(java.lang.String, java.lang.String, java.lang.String)
         */
        @Override
        public void onPMessage(String pattern, String channel, String message) {

        }

        /*
         * (non-Javadoc)
         * 
         * @see redis.clients.jedis.JedisPubSub#onPSubscribe(java.lang.String, int)
         */
        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {

        }

        /*
         * (non-Javadoc)
         * 
         * @see redis.clients.jedis.JedisPubSub#onPUnsubscribe(java.lang.String, int)
         */
        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {

        }

        /*
         * (non-Javadoc)
         * 
         * @see redis.clients.jedis.JedisPubSub#onSubscribe(java.lang.String, int)
         */
        @Override
        public void onSubscribe(String channel, int subscribedChannels) {

        }

        /*
         * (non-Javadoc)
         * 
         * @see redis.clients.jedis.JedisPubSub#onUnsubscribe(java.lang.String, int)
         */
        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {

        }
    }

    /** The pool config. */
    private final JedisPoolConfig poolConfig;

    /** The sentinels. */
    private Set<String> sentinels;

    /** The sentinels threads. */
    private static Set<Thread> sentinelsThreads = new HashSet<Thread>();

    /** The shutdown. */
    private static volatile boolean shutdown = false;

    /** The pool timeout. */
    private int poolTimeout;

    /**
     * Instantiates a new redis sentinel pool.
     *
     * @param poolConfig the pool config
     * @param sentinels the sentinels
     * @param poolTimeout the pool timeout
     */
    public RedisSentinelPool(JedisPoolConfig config, Set<String> sentinels, int poolTimeout) {

        super(config, new jedisFactory());

        this.poolConfig = null;
        this.sentinels = sentinels;
        this.poolTimeout = poolTimeout;

        // life cycle
        RedisSentinelPool.shutdown = false;

        // initializes sentinel listener
        initSentinels();
    }

    /**
     * Unitialize sentinels threads.
     */
    public static void uninit() {

        RedisSentinelPool.shutdown = true;

        // interrupt all threads
        for (Thread thread : RedisSentinelPool.sentinelsThreads) {
            thread.interrupt();
        }

        // releases threads references
        RedisSentinelPool.sentinelsThreads.clear();
    }

    /**
     * Inits a new notifiable for a new master.
     *
     * @param masterName the master name
     * @param notifiable the notifiable
     */
    public void init(String masterName, RedisSentinelNotifiable notifiable) {
        HostAndPort master = getMasterByName(masterName);

        if (master == null) {
            log("Error initializing master" + masterName);
            return;
        }

        // adds a new entry
        initPool(masterName, master, notifiable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see redis.clients.util.Pool#destroy()
     */
    public synchronized void destroy() {
        for (PoolEntry entry : poolMap.values()) {
            if (entry.getPool() != null) {
                entry.getPool().destroy();
            }
        }
    }

    /**
     * Gets the current host master.
     *
     * @param masterName the master name
     * @return the current host master
     */
    public synchronized HostAndPort getCurrentHostMaster(String masterName) {
        PoolEntry entry = poolMap.get(masterName);
        if (entry == null) {
            log("Error retrieving host and port for mastername " + masterName);
            return null;
        }

        return entry.getMaster();
    }

    // Overwrite Pool<Jedis>
    /**
     * Gets the resource.
     *
     * @param masterName the master name
     * @return the resource
     */
    public synchronized Jedis getResource(String masterName) {

        PoolEntry entry = poolMap.get(masterName);
        if (entry == null || entry.getPool() == null) {
            log("Error retrieving jedis pool for mastername " + masterName);
            return null;
        }

        return entry.getPool().getResource();
    }

    /**
     * Inits the pool.
     *
     * @param masterName the master name
     * @param master the master
     * @param newNotifiable the new notifiable
     */
    private synchronized void initPool(String masterName, HostAndPort master, RedisSentinelNotifiable newNotifiable) {

        if (masterName == null || master == null) {
            logger.error("Configuration error: null parameter in initPool");
            return;
        }

        PoolEntry entry = poolMap.get(masterName);

        if (newNotifiable == null && entry.getMaster().equals(master)) {
            log("Master already notified. Ignoring new event for " + master);
            return;
        }

        // checks if already set
        if (entry != null && entry.getMaster() != null && entry.getMaster().equals(master)) {
            log("Pool already created. Reusing pool for master " + master);
        } else {

            JedisPool pool = new JedisPool(poolConfig, master.host, master.port, this.poolTimeout);

            if (entry == null) {
                log("Created pool: " + master);
                entry = new PoolEntry();
                poolMap.put(masterName, entry);
            }

            // sets the new master
            entry.setMaster(master, pool);
        }

        if (newNotifiable != null) {

            // adds a new handler
            entry.addNotifiable(newNotifiable);

            // if we are processing a new notified we just notify the new one
            try {
                newNotifiable.poolChanged(entry.getPool());
            } catch (Throwable e) {
                logger.error("Error notifying pool changing for mastername " + masterName, e);
            }

            return;
        }

        // notifies all handlers
        for (RedisSentinelNotifiable notifiable : entry.getNotifiables()) {
            try {
                notifiable.poolChanged(entry.getPool());
            } catch (Throwable e) {
                logger.error("Error notifying pool changing for mastername " + masterName, e);
            }
        }
    }

    /**
     * Gets the master by name.
     *
     * @param masterName the master name
     * @return the master by name
     */
    private HostAndPort getMasterByName(String masterName) {
        HostAndPort master = null;
        outer: while (RedisSentinelPool.shutdown == false) {
            for (String sentinel : this.sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                final Jedis jedis = new Jedis(hap.host, hap.port, this.poolTimeout);
                try {
                    if (master == null) {
                        List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);
                        if (masterAddr == null || masterAddr.size() < 1) {
                            throw new RuntimeException("Error retrieving master name. Can't find master name from redis sentinel: " + masterName);
                        }
                        master = toHostAndPort(masterAddr);
                        jedis.disconnect();
                        break outer;
                    }
                } catch (JedisConnectionException e) {
                    log("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                } finally {
                    if (jedis != null) {
                        jedis.close();
                    }
                }
            }
            try {
                log("All sentinels down, cannot determinate where is " + masterName + " master is running... sleeping 1000ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted.", e);
            }
        }

        log("Got master running at " + master + ".");

        return master;
    }

    /**
     * Inits the sentinels.
     */
    private void initSentinels() {

        log("Starting sentinel listeners. Number of sentinels = " + this.sentinels.size());

        for (String sentinel : this.sentinels) {

            log("Initializing sentinel thread monitor for sentinel " + sentinel);

            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

            Thread newThread = new Thread() {

                public void run() {
                    while (RedisSentinelPool.shutdown == false) {
                        final Jedis jedis = new Jedis(hap.host, hap.port, poolTimeout);
                        try {
                            jedis.subscribe(new JedisPubSubAdapter() {

                                @Override
                                public void onMessage(String channel, String message) {
                                    // System.out.println(channel + ": " + message);
                                    // +switch-master: mymaster 127.0.0.1 6379 127.0.0.1 6380
                                    log("Sentinel " + hap + " published: " + message + ".");
                                    final String[] switchMasterMsg = message.split(" ");
                                    masterChangeHandler(switchMasterMsg[0], toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4])));
                                }
                            }, "+switch-master");
                        } catch (JedisConnectionException e) {
                            log("Lost connection to " + hap + ". Sleeping 5000ms.");
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e1) {
                                log("Thread interrupted");
                            }
                        } catch (Throwable e) {
                            if (RedisSentinelPool.shutdown) {
                                log("Terminanting sentinel monitoring thread");
                            }

                            // not interrupted. Dumps error
                            logger.error("Error in sentinel monitoring thread", e);
                        } finally {
                            if (jedis != null) {
                                jedis.close();
                            }
                        }

                    }
                };
            };

            // adds the thread to the set set
            RedisSentinelPool.sentinelsThreads.add(newThread);

            // start threads
            newThread.setDaemon(true);
            newThread.start();
        }
    }

    /**
     * Master change handler.
     *
     * @param masterName the master name
     * @param newMaster the new master
     */
    private void masterChangeHandler(String masterName, HostAndPort newMaster) {

        // re-initializes
        initPool(masterName, newMaster, null);
    }

    /**
     * Log.
     *
     * @param msg the msg
     */
    private void log(String msg) {
        logger.info("Jedis Sentinel Pool: " + msg);
    }

    /**
     * Return resource object.
     *
     * @param masterName the master name
     * @param resource the resource
     */
    public synchronized void returnResourceObject(String masterName, final Jedis resource) {
        PoolEntry entry = poolMap.get(masterName);
        if (entry == null || entry.getPool() == null) {
            log("Error retrieving pool entry for mastername " + masterName);
        }

        entry.getPool().returnResourceObject(resource);
    }

    /**
     * To host and port.
     *
     * @param getMasterAddrByNameResult the get master addr by name result
     * @return the host and port
     */
    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        final HostAndPort hap = new HostAndPort();
        hap.host = getMasterAddrByNameResult.get(0);
        hap.port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return hap;
    }

    /**
     * Gets the pool.
     *
     * @param masterName the master name
     * @return the pool
     */
    public synchronized JedisPool getPool(String masterName) {
        PoolEntry entry = poolMap.get(masterName);
        if (entry == null || entry.getPool() == null) {
            log("Error retrieving pool entry for mastername " + masterName);
        }

        return entry.getPool();
    }
}
