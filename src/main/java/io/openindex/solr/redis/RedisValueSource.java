package io.openindex.solr.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;

import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisValueSource extends ValueSource {
    final String idField;
    final static String redisHost;
    final static int redisPort;
    final String redisKey;
    final static String solrDataHome;

    private static final Logger LOG = LoggerFactory.getLogger(RedisValueSource.class);
    private static final int REDIS_READ_TIMEOUT = 20000;
    private static final int REDIS_WRITE_TIMEOUT = 500;
    private static final long REDIS_RESULT_CACHE_SIZE = 10000L;
    private static final long REDIS_RESULT_CACHE_TTL_MS = 60;

    protected static final JedisPool pool;
    // agam
    protected static final ConcurrentMap<String, Object2DoubleOpenHashMap<String>> cache;
    protected static boolean KEEP_RUNNING = true;
    private static Map<String, Long> lastUpdatedTime = new HashMap<>();
    private static final Pattern CORENAME_PATTERN = Pattern.compile("(core[0-9]+)");
    // SCAN KEY
    private static final String KEY_FOR_UPDATES_IN_REDIS = "klevuredis:updatedKeys:";
    private static String SCAN_INIT_CURSOR = null;
    private static int counter = 0;


    public RedisValueSource(String idField, String redisKey) {
        //LOG.info("Redis-plugin: New instance is created");
        this.idField = idField;
        this.redisKey = redisKey.intern();
    }

    static {
        redisHost = "localhost"; // TODO: parametrise
        redisPort = 6879; // TODO: parametrise
        solrDataHome = System.getProperty("solr.solr.home", "/home/apps/solr-home/");
        // TODO: read in-place cores and load those specific collections ONLY!

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxWaitMillis(REDIS_WRITE_TIMEOUT);
        jedisPoolConfig.setMaxWaitMillis(500);
        jedisPoolConfig.setBlockWhenExhausted(false);
        // maximum 2K connections at a time
        jedisPoolConfig.setMaxTotal(2000);
        // setting this to negative (i.e. no eviction policy)
        jedisPoolConfig.setMaxIdle(-1);

        pool = new JedisPool(jedisPoolConfig, redisHost, redisPort, REDIS_READ_TIMEOUT);

        LOG.info("Redis-plugin : Connecting to redis instance @ " + redisHost + ":" + redisPort);

        // agam
        cache = CacheBuilder.newBuilder().maximumSize(REDIS_RESULT_CACHE_SIZE)
                .expireAfterWrite(REDIS_RESULT_CACHE_TTL_MS, TimeUnit.DAYS)
                .<String, Object2DoubleOpenHashMap<String>>build().asMap();

        refreshRedisDataThread();
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
        long startTime = System.currentTimeMillis();
        final BinaryDocValues lookup = DocValues.getBinary(readerContext.reader(), idField);

        Object2DoubleOpenHashMap<String> values = null;
        Object2DoubleOpenHashMap<String> clookup;

        // Cache lookup
        clookup = cache.get(redisKey);
        // if cache is not present for the redis key then
        if (clookup == null) {
            // first synchronized the key so when other segments will call this method it will block those request.
            synchronized (redisKey) {
                // one more time check if the cache is present (It maybe possible that other segment has already added the redisKey into cache)
                clookup = cache.get(redisKey);
                // if cache is still not found then call the methods to retrive the data from redis
                if (clookup == null) {
                    // if call is for bulkboost or self-learning score then do not add it into cache just return it's value.
                    // why: bulk & self learning scores are called from kmc (not from search calls)
                    if (redisKey.contains("bulkboost") || redisKey.contains("originalSelflearning")) {
                        clookup = getVales(redisKey);
                    } else {
                        // cache only appliedScores and ManualBoost scores.
                        setCacheVales(redisKey);
                        clookup = cache.get(redisKey);
                    }
                }
            }
        }

        // Assign the object reference of clookup to values
        values = clookup;

        // agam
        final Object2DoubleOpenHashMap<String> vals = values;
        DoubleDocValues toReturn = new DoubleDocValues(this) {
            @Override
            public double doubleVal(int i) throws IOException {
                Double val = vals.getDouble(this.translateId(i));
                //LOG.info("Redis-plugin : value for id {} is {} ", this.translateId(i), val);
                // If key value is not found then return 1 Default
                return val = (val == null || val <= 0.0) ? 1.0 : (double) val;
            }

            public String translateId(int id) throws IOException {
                BytesRef ref = new BytesRef();
                // BinaryDocValues advance to exact target, and ref is
                // read this is optimisation done to get sorted doc
                // values and lookups becomes faster.
                if (lookup.advanceExact(id)) {
                    ref = lookup.binaryValue();
                }
                return ref.utf8ToString();
            }
        };
        return toReturn;
    }

    // FIXME: on shutdown, close JedisPool

    @Override
    public String description() {
        return "redis(" + idField + "," + redisKey + ")";
    }

    @Override
    public int hashCode() {
        // FIXME
        return redisKey.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        // FIXME
        return true;
    }

    /*
     * This method is used to refresh the Redis data on every minutes
     *
     *
     * */
    private static void refreshRedisDataThread() {
        try {
            LOG.debug("Redis-plugin : refreshRedisDataThread...");
            //System.out.println("Redis-plugin : Cache value refresh Thread called...");
            final Thread reloadThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (KEEP_RUNNING) {
                        //System.out.println("Redis-plugin : Cache getCoreNamesTobeUpdated send...");
                        try {
                            getCollectionTobeUpdated();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            // Sleeping cache refresh thread for 5 sec
                            Thread.sleep(5 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            reloadThread.setName("Thread - Redis-Plugin: Reload Cache");
            reloadThread.start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    KEEP_RUNNING = false;
                    reloadThread.interrupt();
                }
            });
        } catch (Exception e) {
            LOG.error("Redis-plugin : Exception during refreshRedisDataThread ==> {}", e.getMessage());
        }
    }


    /*
     * This method is used to update the cache for given redisKey
     * */
    public static void setCacheVales(String localRedisKey) {
        long startTime = System.currentTimeMillis();
        Object2DoubleOpenHashMap<String> values = null;
        // Get new jedis connection from pool
        try (Jedis jedis = pool.getResource()) {
            // get values of the given key from redis
            Set<Tuple> tuples = null;
            tuples = jedis.zrangeByScoreWithScores(localRedisKey, 0, Integer.MAX_VALUE);
            // Assign size to Map
            values = new Object2DoubleOpenHashMap<>(tuples.size());
            // Traverse the Tuple and add it to Map
            for (Tuple t : tuples) {
                // If the score is not 1.0 then only add it to Map (Skip default values!)
                if (t.getScore() != 1.0) {
                    values.put(t.getElement(), t.getScore());
                }
            }
            if (values == null || values.isEmpty()) {
                values = new Object2DoubleOpenHashMap<>();
            }
            // synchronize the redisKey before updating it into cache
            synchronized (localRedisKey) {
                cache.put(localRedisKey, values);
            }
            LOG.info("Redis-plugin : CACHE updated for key \"{}\" in {} ms for {} records", localRedisKey, (System.currentTimeMillis() - startTime), tuples.size());
        } catch (JedisConnectionException e) {
            LOG.error("Redis-plugin : Could not query redis for key {} -> {}", localRedisKey, e.getMessage());
        } catch (JedisDataException e) {
            LOG.error("Redis-plugin : Wrong redis data type for key {} -> {}", localRedisKey, e.getMessage());
        } catch (Exception e) {
            LOG.error("Redis-plugin : Exception during redis query for key {} -> {}", localRedisKey, e.getMessage());
        }
    }

    /*
     * This method is used to get the values directly from the redis cache (instead from the cache)
     * */
    public static Object2DoubleOpenHashMap<String> getVales(String redisKey) {
        long startTime = System.currentTimeMillis();
        Object2DoubleOpenHashMap<String> values = null;
        // Get new jedis connection from pool
        try (Jedis jedis = pool.getResource()) {
            // get values of the given key from redis
            Set<Tuple> tuples = null;
            tuples = jedis.zrangeByScoreWithScores(redisKey, 0, Integer.MAX_VALUE);
            // Assign size to Map
            values = new Object2DoubleOpenHashMap<>(tuples.size());
            // Traverse the Tuple and add it to Map
            for (Tuple t : tuples) {
                // If the score is not 1.0 then only add it to Map (Skip default values!)
                if (t.getScore() != 1.0) {
                    values.put(t.getElement(), t.getScore());
                }
            }
            if (values == null || values.isEmpty()) {
                values = new Object2DoubleOpenHashMap<>();
            }
            LOG.info("Redis-plugin : Get values of key \"{}\" in {} ms for {} records", redisKey, (System.currentTimeMillis() - startTime), tuples.size());
        } catch (JedisConnectionException e) {
            LOG.error("Redis-plugin : Get values Could not query redis for key {} -> {}", redisKey, e.getMessage());
        } catch (JedisDataException e) {
            LOG.error("Redis-plugin : Get values Wrong redis data type for key {} -> {}", redisKey, e.getMessage());
        } catch (Exception e) {
            LOG.error("Redis-plugin : Get values Exception during redis query for key {} -> {}", redisKey, e.getMessage());
        }
        return values;
    }

    public static List<String> getCollectionNames(final File solrHome) {
        List<String> collectionNames = new ArrayList<>();
        for (final File collectionDataDir : solrHome.listFiles()) {
            if (collectionDataDir.isDirectory()) {
                // list directories with keywords 'core', 'shard', 'replica'
                // essentially core directories
                if (collectionDataDir.getName().contains("core") && collectionDataDir.getName().contains("shard") &&
                        collectionDataDir.getName().contains("replica")) {
                    for (final File coreFile : solrHome.listFiles()) {
                        // read core.properties and get 'collection' name
                        if (coreFile.getName().equals("core.properties")) {
                            Properties coreProp = new Properties();
                            try {
                                coreProp.load(new FileInputStream(coreFile.getAbsolutePath()));
                            }
                            catch (IOException ex) {
                                // don't throw an error, since this process can happen
                                // when a collection is getting deleted
                                LOG.error("core.properties unreadable at {}", coreFile.getAbsolutePath());
                            }
                            collectionNames.add(coreProp.getProperty("collection"));
                        }
                    }
                }
            }
        }
        LOG.info("collections for which redis cache to be refresh: {}", collectionNames);
        return collectionNames;
    }

    /*
     * This method used for,
     * 1. When Thread is started first time then it will load all cores data into cache.
     * 2. For subsequent calls from thread, it will first SCAN the redis key, and update only those cores boosting score which are recently updated.
     * */
    private static void getCollectionTobeUpdated() throws IOException {
        System.out.println("Redis-plugin : Cache value getCollectionTobeUpdated...");
        // First fetch the list of all the cores list
        List<String> coreNames = getCollectionNames(new File(solrDataHome));
        // Get the Jedis connection from redis pool
        try (Jedis jedis = pool.getResource()) {
            // If method is called by subsequent calls then,
            if (SCAN_INIT_CURSOR != null) {
                LOG.debug("Redis-plugin : inside method getCollectionTobeUpdated");
                // 1. Scan for the updated keys
                ScanResult<String> newUpdatedKeys = null;
                SCAN_INIT_CURSOR = "0";
                // clear core names list, so we will add only those cores which we will found in updated keys
                coreNames.clear();
                // Start SCAN the cursor until 0
                do {
                    newUpdatedKeys = jedis.scan(SCAN_INIT_CURSOR, new ScanParams().match(KEY_FOR_UPDATES_IN_REDIS + "core*"));
                    SCAN_INIT_CURSOR = String.valueOf(Integer.parseInt(newUpdatedKeys.getCursor()));
                    //System.out.println("Redis-plugin : Cache SCAN_INIT_CURSOR : " + SCAN_INIT_CURSOR);
                    if (newUpdatedKeys != null && newUpdatedKeys.getCursor() != null && !newUpdatedKeys.getResult().isEmpty()) {
                        // Iterate through each key which you find in scan
                        for (String keys : newUpdatedKeys.getResult()) {
                            try {
                                String coreName = null;
                                // Get the core name from key
                                Matcher coreNameMatcher = CORENAME_PATTERN.matcher(keys);
                                if (coreNameMatcher.find()) {
                                    coreName = coreNameMatcher.group(0);
                                    // Get the last updated time stamp from redis
                                    Long timeStamp = Long.parseLong(jedis.get(KEY_FOR_UPDATES_IN_REDIS + coreName));
                                    // Get the last updated timeStamp from Map
                                    if (lastUpdatedTime.containsKey(coreName)) {
                                        Long lastUpdatedTimeStamp = lastUpdatedTime.get(coreName);
                                        LOG.debug("Redis-plugin : lastUpdatedTimeStamp {}", lastUpdatedTimeStamp);
                                        LOG.debug("Redis-plugin : timeStamp {}", timeStamp);
                                        // If the redis updated timestamp is bigger than last updated TimeStamp then add this coreName into list
                                        if (!timeStamp.equals(lastUpdatedTimeStamp)) {
                                            coreNames.add(coreName);
                                            // Update timestamp in Map
                                            lastUpdatedTime.put(coreName, timeStamp);
                                        }
                                    } else {
                                        coreNames.add(coreName);
                                        lastUpdatedTime.put(coreName, timeStamp);
                                    }
                                }
                            } catch (Exception e) {
                                LOG.error("Redis-plugin : Exception during preparing the core list for ==> {} \n {}", keys, e.getMessage());
                            }
                        }
                    }
                    LOG.debug("Redis-plugin : Scan {} - {} - {}", SCAN_INIT_CURSOR, newUpdatedKeys.getCursor(), newUpdatedKeys.getResult().size());
                } while (!SCAN_INIT_CURSOR.equals("0"));
                // If the results are found in the SCAN then
                if (coreNames != null && !coreNames.isEmpty()) {
                    // Execute the refresh cache method for the filtered coreNames
                    refreshCacheForAllCores(coreNames);
                }
            } else if (SCAN_INIT_CURSOR == null) {
                // reload for all the cores
                refreshCacheForAllCores(coreNames);
                SCAN_INIT_CURSOR = "0";
                ScanResult<String> newUpdatedKeys = jedis.scan(SCAN_INIT_CURSOR, new ScanParams().match(KEY_FOR_UPDATES_IN_REDIS + "core*"));
            }
        } catch (Exception e) {
            LOG.error("Redis-plugin : Exception during refreshRedisDataThread ==> {}", e.getMessage());
        }
    }

    /*
     * This method is used to update the cache of the given coreNames
     * */
    private static void refreshCacheForAllCores(final List<String> coreNames) {
        try {
            if (coreNames == null || coreNames.isEmpty()) {
                //LOG.info("Redis-plugin : No cores are found to update the cache.");
                return;
            }
            for (String coreName : coreNames) {
                try {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(new Date());
                    long updatedTime = calendar.getTimeInMillis();
                    String appliedKey = "klevusearch:_all:" + coreName;
                    String manualKeys = "klevusearch:manual:_all:" + coreName;
                    String selfLearningKeys = "klevusearch:selflearning:_all:" + coreName;
                    setCacheVales(appliedKey);
                    setCacheVales(manualKeys);
                    setCacheVales(selfLearningKeys);
                    //System.out.println("Redis-plugin : Cache value updated for " + coreName + " at " + new Date() + " :: " + updatedTime);
                    LOG.info("Redis-plugin : Scan {} - {} - {}", coreName, new Date(), updatedTime);
                } catch (Exception e) {
                    LOG.error("Redis-plugin : Exception while updating cache for ==> {}", coreName);
                    // If any exception occur then reset the lastUpdatedTime in Map
                    lastUpdatedTime.put(coreName, 0L);
                }
            }
        } catch (Exception e) {
            LOG.error("Redis-plugin : Exception in refreshCacheForAllCores method ==> {}", e.getMessage());
        }
    }
}