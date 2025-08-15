package org.lucee.extension.couchbase;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.couchbase.coder.Coder;
import org.lucee.extension.couchbase.coder.JSON;
import org.lucee.extension.couchbase.util.CacheUtil;
import org.lucee.extension.couchbase.util.CouchbaseUtil;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.PlanningFailureException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryResult;

import lucee.commons.io.cache.CacheEntry;
import lucee.commons.io.cache.CacheKeyFilter;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.config.Config;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Creation;

public class Couchbase extends CacheSupport {

	public static short TRANSCODER_JSON = 1;
	public static short TRANSCODER_BINARY = 2;
	public static long MAX_USAGE = 300L * 1000L;

	private Key defaultKey;
	private String connectionKey;
	private String connectionString;
	private String username;
	private String password;
	private String bucketName;
	private Cluster cluster;
	private String scopeName;
	private String collectionName;
	private boolean createIfNecessaryBucket = false;
	private boolean createIfNecessaryScope = false;
	private boolean createIfNecessaryCollection = false;

	private short transcoder;
	private long defaultExpire;
	private long connectionTimeout;
	private Map<String, Collection> collections = new ConcurrentHashMap<>();
	private Map<String, Long> collectionsCreated = new ConcurrentHashMap<>();
	private Map<String, Bucket> buckets = new ConcurrentHashMap<>();
	private Map<String, Scope> scopes = new ConcurrentHashMap<>();
	private ClassLoader cl;
	private boolean hasBucket;
	private boolean hasScope;
	private boolean hasCollection;

	public Couchbase(String connectionString, String username, String password, String bucketName, String scopeName, String collectionName, short transcoder, long defaultExpire,
			long connectionTimeout, boolean createIfNecessaryBucket, boolean createIfNecessaryScope, boolean createIfNecessaryCollection) throws CouchbaseException {
		this.connectionString = connectionString;
		this.username = username;
		this.password = password;
		this.bucketName = bucketName;
		this.scopeName = scopeName;
		this.collectionName = collectionName;
		this.transcoder = transcoder;
		this.defaultExpire = defaultExpire;
		this.connectionTimeout = connectionTimeout;

		this.createIfNecessaryBucket = createIfNecessaryBucket;
		this.createIfNecessaryScope = createIfNecessaryScope;
		this.createIfNecessaryCollection = createIfNecessaryCollection;

		defaultKey = new Key(null, bucketName, scopeName, collectionName);// better impl
		setAndValidate();
	}

	public Couchbase() {
		// defaultKey = new Key(null, bucketName, scopeName, collectionName);// better impl
	}

	@Override
	public void init(Config config, String cacheName, Struct arguments) throws IOException {
		init(config, arguments);
	}

	public void init(Struct arguments) throws IOException {
		init(null, arguments);
	}

	public void init(Config config, Struct arguments) throws IOException {
		this.cl = arguments.getClass().getClassLoader();

		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		// this.cl = arguments.getClass().getClassLoader();
		if (config == null) config = CFMLEngineFactory.getInstance().getThreadConfig();

		try {
			connectionString = caster.toString(arguments.get("connectionString"));

			username = caster.toString(arguments.get("user", null), null);
			if (Util.isEmpty(username)) username = caster.toString(arguments.get("username"));

			password = caster.toString(arguments.get("password"));

			bucketName = caster.toString(arguments.get("bucket", null), null);
			if (Util.isEmpty(bucketName)) bucketName = caster.toString(arguments.get("bucketName"));

			scopeName = caster.toString(arguments.get("scope", null), null);
			if (Util.isEmpty(scopeName)) scopeName = caster.toString(arguments.get("scopeName"));

			collectionName = caster.toString(arguments.get("collection", null), null);
			if (Util.isEmpty(collectionName)) collectionName = caster.toString(arguments.get("collectionName"));

			transcoder = CouchbaseUtil.toTranscoder(caster.toString(arguments.get("transcoder", "object"), "object"), Couchbase.TRANSCODER_BINARY);

			defaultExpire = caster.toLong(arguments.get("defaultExpire", 0L), 0L);

			connectionTimeout = caster.toLong(arguments.get("connectionTimeout", 0L), 0L);

			this.createIfNecessaryBucket = caster.toBooleanValue(arguments.get("createIfNecessaryBucket", false), false);
			this.createIfNecessaryScope = caster.toBooleanValue(arguments.get("createIfNecessaryScope", false), false);
			this.createIfNecessaryCollection = caster.toBooleanValue(arguments.get("createIfNecessaryCollection", false), false);
		}
		catch (PageException pe) {
			throw engine.getExceptionUtil().toIOException(pe);
		}
		defaultKey = new Key(null, bucketName, scopeName, collectionName);// better impl
		setAndValidate();

	}

	@Override
	public CacheEntry getCacheEntry(String rawKey) throws IOException {
		Key k = toKey(rawKey);
		try {
			return new CouchbaseEntry(this, getCollection(k).get(k.key, getOptions()), k.key, transcoder);
		}
		catch (Exception e) {
			collectionsCreated.put(k.bsc(), 0L); // force a new collection
			throw new CouchbaseException("could not retrieve value for [" + fullKey(k) + "]", e);
		}
	}

	@Override
	public CacheEntry getCacheEntry(String rawKey, CacheEntry defaultValue) {
		Key k;
		try {
			k = toKey(rawKey);
		}
		catch (Exception e) {
			return defaultValue;
		}

		try {
			return new CouchbaseEntry(this, getCollection(k).get(k.key, getOptions()), k.key, transcoder);
		}
		catch (Exception e) {
			collectionsCreated.put(k.bsc(), 0L); // force a new collection
			return defaultValue;
		}
	}

	private GetOptions getOptions() {
		GetOptions options = GetOptions.getOptions();
		if (transcoder == TRANSCODER_BINARY) {
			options.transcoder(RawBinaryTranscoder.INSTANCE);
		}
		options.withExpiry(true);
		return options;
	}

	@Override
	public void put(String rawKey, Object val, Long idle, Long live) throws IOException {
		Key k = toKey(rawKey);
		Object value;

		UpsertOptions options = UpsertOptions.upsertOptions();
		if (transcoder == TRANSCODER_BINARY) {
			options.transcoder(RawBinaryTranscoder.INSTANCE);
			value = Coder.serialize(val);

		}
		else {// if (transcoder == Couchbase.TRANSCODER_JSON)
			value = JSON.toJsonObject(val);
		}

		// expire
		long exp;
		if (live != null && live.longValue() > 0) {
			exp = live.longValue();
		}
		else if (idle != null && idle.longValue() > 0) {
			exp = idle.longValue();
		}
		else {
			exp = defaultExpire;
		}

		if (exp > 0) options.expiry(Duration.ofMillis(exp));

		MutationResult result = getCollection(k).upsert(k.key, value, options);
		// TODO check result for error and throw exception if necessary
	}

	@Override
	public boolean contains(String rawKey) throws IOException {
		Key k = toKey(rawKey);
		return getCollection(k).exists(k.key, ExistsOptions.existsOptions()).exists();
	}

	@Override
	public boolean remove(String rawKey) throws IOException {
		Key k = toKey(rawKey);
		// RemoveOptions.removeOptions();
		try {
			getCollection(k).remove(k.key);
			return true;
		}
		catch (DocumentNotFoundException dnfe) {
			return false;
		}
	}

	@Override
	public List<String> keys() throws IOException {
		try {
			return _keys(null);
		}
		catch (IOException ioe) {
			throw ioe;
		}
	}

	@Override
	public List<String> keys(CacheKeyFilter filter) throws IOException {
		// TODO is that condition ok
		try {
			return _keys(CacheUtil.isWildCardFiler(filter) ? filter.toPattern() : null);
		}
		catch (IOException ioe) {
			throw ioe;
		}
	}

	private List<String> _keys(String criteria) throws IOException {
		if (!Util.isEmpty(criteria, true)) criteria = " where META().id like  \"" + CouchbaseUtil.toSQL(criteria) + "\"";
		else criteria = "";
		if (Util.isEmpty(collectionName)) throw new CouchbaseException(
				"this cache defintion has not defined any collection, so there is no base to list keys, only cache defintions that have a collection name can do this.");
		String sql = "select META().id as id from `" + collectionName + "`" + criteria + " order by id";
		// print.e(sql);
		QueryResult res;
		try {
			res = getScope(defaultKey).query(sql);
		}
		catch (PlanningFailureException pfe) {
			if (("" + pfe.getMessage()).indexOf("No index available") == -1) throw pfe;
			getScope(defaultKey).query("create primary index " + collectionName + "_idx on `" + collectionName + "`");
			res = getScope(defaultKey).query(sql);
		}
		// final QueryResult res = getScope().query(sql);// , queryOptions().metrics(true));
		List<String> keys = new ArrayList<String>();
		for (JsonObject row: res.rowsAsObject()) {
			keys.add(row.getString("id"));
		}
		return keys;
	}

	public CouchbaseMeta meta(String rawKey) throws IOException {
		Key k = toKey(rawKey);
		String sql = "select META().id as id,META().flags as flags,META().expiration as expiration,META().cas as cas,META().type as type from `" + k.collection
				+ "` where META().id=\"" + k.key + "\" ";
		QueryResult res;
		try {
			res = getScope(k).query(sql);
		}
		catch (PlanningFailureException pfe) {
			if (("" + pfe.getMessage()).indexOf("No index available") == -1) throw pfe;
			getScope(k).query("create primary index " + k.collection + "_idx on `" + k.collection + "`");
			res = getScope(k).query(sql);
		}
		// final QueryResult res = getScope().query(sql);// , queryOptions().metrics(true));
		// print.e(res.toString());
		for (JsonObject row: res.rowsAsObject()) {
			return new CouchbaseMeta(row.getString("id"),

					row.getInt("flags"),

					row.getLong("expiration"),

					row.getLong("cas"),

					row.getString("type"));
			// keys.add(row.getString("id"));
		}
		if (!contains(rawKey)) throw new CouchbaseException("could not load meta data for [" + fullKey(k) + "], because it does not exist.");
		throw new CouchbaseException("could not load meta data for [" + fullKey(k) + "]");
	}

	@Override
	public Struct getCustomInfo() throws IOException {
		// TODO Auto-generated method stub
		Struct info = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		if (!Util.isEmpty(bucketName)) info.setEL("defaultBucket", bucketName);
		if (!Util.isEmpty(scopeName)) info.setEL("defaultScope", scopeName);
		if (!Util.isEmpty(collectionName)) info.setEL("defaultCollection", collectionName);

		// all bucket info
		{
			Creation util = CFMLEngineFactory.getInstance().getCreationUtil();
			Struct buckets = util.createStruct();
			Struct bucket;
			Entry<String, BucketSettings> e;
			BucketSettings bs;
			BucketManager bucketManager = buckets();
			Iterator<Entry<String, BucketSettings>> it = bucketManager.getAllBuckets().entrySet().iterator();
			while (it.hasNext()) {
				e = it.next();
				bs = e.getValue();
				bucket = util.createStruct();
				bucket.setEL("name", e.getKey());
				bucket.setEL("type", bs.bucketType().name());
				bucket.setEL("compressionMode", bs.compressionMode().name());
				bucket.setEL("conflictResolutionType", bs.conflictResolutionType().name());
				bucket.setEL("evictionPolicy", bs.evictionPolicy().name());
				bucket.setEL("flushEnabled", bs.flushEnabled());
				bucket.setEL("healthy", bs.healthy());
				bucket.setEL("maxExpiryInSeconds", bs.maxExpiry().getSeconds());
				bucket.setEL("maxExpiryInNano", bs.maxExpiry().getNano());
				bucket.setEL("minimumDurabilityLevel", bs.minimumDurabilityLevel().name());
				bucket.setEL("numReplicas", bs.numReplicas());
				bucket.setEL("ramQuotaMB", bs.ramQuotaMB());
				bucket.setEL("replicaIndexes", bs.replicaIndexes());
				// bucket.put("", bs.storageBackend().alias());
				buckets.setEL(e.getKey(), bucket);
			}
			info.setEL("buckets", buckets);
		}

		return info;
	}

	@Override
	public long hitCount() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long missCount() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CacheEntry getQuiet(String key, CacheEntry defaultValue) {
		// TODO Auto-generated method stub
		return getCacheEntry(key, defaultValue);
	}

	private Bucket getBucket(Key key) throws CouchbaseException {
		Bucket b;
		if ((b = buckets.get(key.b())) == null) {
			synchronized (CouchbaseFactory.getToken("bucket:" + key.b())) {
				if ((b = buckets.get(key.b())) == null) {
					String bn = key.bucket != null ? key.bucket : bucketName;
					if (!existsBucket(bn)) {
						if (!createIfNecessaryBucket) throw new CouchbaseException("there is no bucket with name [" + bn + "]");
						throw new CouchbaseException("creating buckets is not supported yet");// TODO createBucket(bn);

					}
					buckets.put(key.b(), b = bucket(bn));

				}
			}
		}
		return b;
		// return bucket.scope(this.scope).collection(this.collection);

	}

	private Scope getScope(Key key) throws CouchbaseException {
		Scope s;
		if ((s = scopes.get(key.bs())) == null) {
			synchronized (CouchbaseFactory.getToken("scope:" + key.bs())) {
				if ((s = scopes.get(key.bs())) == null) {
					String sn = key.scope != null ? key.scope : this.scopeName;
					long start = System.currentTimeMillis();
					if (!existsScope(key.bucket, sn)) {
						if (!createIfNecessaryScope) throw new CouchbaseException("there is no scope with name [" + sn + "] in the bucket [" + key.bucket + "]");
						getBucket(key).collections().createScope(sn);
						// throw new CouchbaseException("creating scope is not supported yet");// TODO createBucket(bn);

					}
					scopes.put(key.bs(), s = getBucket(key).scope(sn));
				}
			}
		}
		return s;
		// return bucket.scope(this.scope).collection(this.collection);

	}

	private Collection getCollection(Key key) throws CouchbaseException {
		Collection c;
		if ((c = collections.get(key.bsc())) == null || (getCollectionCreated(key.bsc()) + MAX_USAGE) < System.currentTimeMillis()) {
			synchronized (CouchbaseFactory.getToken("collection:" + key.bsc())) {
				if ((c = collections.get(key.bsc())) == null || (getCollectionCreated(key.bsc()) + MAX_USAGE) < System.currentTimeMillis()) {
					String cn = key.collection != null ? key.collection : this.collectionName;
					if (!existsCollection(key.bucket, key.scope, cn)) {
						if (!createIfNecessaryCollection) throw new CouchbaseException("there is no collection with name [" + cn + "] in  [" + key.bs() + "]");

						if (!existsScope(key.bucket, key.scope)) getBucket(key).collections().createScope(key.scope);

						getBucket(key).collections().createCollection(CollectionSpec.create(cn, key.scope));
						// throw new CouchbaseException("failed to create collection [" + cn + "] in [" + key.bs() + "]
						// creating collection is not supported yet");

					}
					collections.put(key.bsc(), c = getScope(key).collection(key.collection != null ? key.collection : this.collectionName));
					collectionsCreated.put(key.bsc(), System.currentTimeMillis());
				}
			}
		}
		return c;

	}

	public boolean existsCollection(String bucketName, String scopeName, String collectionName) throws CouchbaseException {
		if (Util.isEmpty(bucketName, true)) throw new CouchbaseException("invalid bucket name, bucket name is an empty string");
		bucketName = bucketName.trim();
		if (Util.isEmpty(scopeName, true)) throw new CouchbaseException("invalid scope name, scope name is an empty string");
		scopeName = scopeName.trim();
		if (Util.isEmpty(collectionName, true)) throw new CouchbaseException("invalid scope name, scope name is an empty string");
		collectionName = collectionName.trim();

		Bucket bucket = bucket(bucketName);
		CollectionManager collectionManager = bucket.collections();
		for (ScopeSpec scope: collectionManager.getAllScopes()) {
			if (scopeName.equals(scope.name())) {
				for (CollectionSpec collection: scope.collections()) {
					if (collectionName.equals(collection.name())) return true;
				}
			}
		}
		return false;
	}

	public List<String> listCollections(String bucketName, String scopeName) throws CouchbaseException {
		if (Util.isEmpty(bucketName, true)) throw new CouchbaseException("invalid bucket name, bucket name is an empty string");
		bucketName = bucketName.trim();
		if (Util.isEmpty(scopeName, true)) throw new CouchbaseException("invalid scope name, scope name is an empty string");
		scopeName = scopeName.trim();

		List<String> collectionNames = new ArrayList<>();
		Bucket bucket = bucket(bucketName);
		CollectionManager collectionManager = bucket.collections();
		boolean match = false;
		for (ScopeSpec scope: collectionManager.getAllScopes()) {
			if (scopeName.equals(scope.name())) {
				match = true;
				for (CollectionSpec collection: scope.collections()) {
					collectionNames.add(collection.name());
				}
			}
		}
		if (!match) throw new CouchbaseException("there is no scope with name [" + scopeName + "] in the bucket [" + bucketName + "]");
		return collectionNames;
	}

	public boolean existsScope(String bucketName, String scopeName) throws CouchbaseException {
		if (Util.isEmpty(bucketName, true)) throw new CouchbaseException("invalid bucket name, bucket name is an empty string");
		bucketName = bucketName.trim();
		if (Util.isEmpty(scopeName, true)) throw new CouchbaseException("invalid scope name, scope name is an empty string");
		scopeName = scopeName.trim();

		Bucket bucket = bucket(bucketName);
		CollectionManager collectionManager = bucket.collections();
		for (ScopeSpec scope: collectionManager.getAllScopes()) {
			if (scopeName.equals(scope.name())) return true;
		}
		return false;
	}

	public List<String> listScopes(String bucketName) throws CouchbaseException {
		if (Util.isEmpty(bucketName, true)) throw new CouchbaseException("invalid bucket name, bucket name is an empty string");
		bucketName = bucketName.trim();
		List<String> scopeNames = new ArrayList<>();
		Bucket bucket = bucket(bucketName);
		CollectionManager collectionManager = bucket.collections();
		for (ScopeSpec scope: collectionManager.getAllScopes()) {
			scopeNames.add(scope.name());
		}
		return scopeNames;
	}

	public boolean existsBucket(String bucketName) throws CouchbaseException {
		if (Util.isEmpty(bucketName, true)) throw new CouchbaseException("invalid bucket name, bucket name is an empty string");
		bucketName = bucketName.trim();
		BucketManager bucketManager = buckets();
		for (String name: bucketManager.getAllBuckets().keySet()) {
			if (bucketName.equals(name)) return true;
		}
		return false;
	}

	public List<String> listBuckets() {
		List<String> bucketNames = new ArrayList<>();
		BucketManager bucketManager = buckets();
		for (String name: bucketManager.getAllBuckets().keySet()) {
			bucketNames.add(name);
		}
		return bucketNames;
	}

	private Cluster getCluster(boolean force) {
		if (force || cluster == null) {

			if (connectionKey == null) this.connectionKey = ("" + connectionString + username + password).intern();

			synchronized (connectionKey) {
				if (force || cluster == null) {

					// close old connection
					Cluster tmp = cluster;
					if (force && tmp != null) {
						try {
							tmp.close();
						}
						catch (Exception ex) {
						}
					}

					tmp = Cluster.connect(connectionString,

							ClusterOptions.clusterOptions(username, password).environment(env -> {
								// Sets a pre-configured profile called "wan-development" to help avoid
								// latency issues when accessing Capella from a different Wide Area Network
								// or Availability Zone (e.g. your laptop).
								env.applyProfile("wan-development");

								if (connectionTimeout > 0L) {
									env.timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(connectionTimeout)).connectTimeout(Duration.ofMillis(connectionTimeout)));
								}

							}));
					tmp.waitUntilReady(Duration.ofMillis(connectionTimeout));
					cluster = tmp;
				}
			}
		}
		return cluster;
	}

	private Bucket bucket(String bucketName) {
		Cluster cluster = null;
		try {
			cluster = getCluster(false);
			return cluster.bucket(bucketName);
		}
		catch (Exception e) {
			// validate
			try {
				cluster.ping(); // in case ping works, exception is unrelated to connection
				throw toRuntimeException(e);
			}
			catch (Exception ex) {

			}
			// try again and force a new connection
			return getCluster(true).bucket(bucketName);
		}
	}

	private BucketManager buckets() {
		Cluster cluster = null;
		try {
			cluster = getCluster(false);
			return cluster.buckets();
		}
		catch (Exception e) {
			// validate
			try {
				cluster.ping(); // in case ping works, exception is unrelated to connection
				throw toRuntimeException(e);
			}
			catch (Exception ex) {

			}
			// try again and force a new connection
			return getCluster(true).buckets();
		}
	}

	private RuntimeException toRuntimeException(Exception e) {
		if (e instanceof RuntimeException) return (RuntimeException) e;
		return new RuntimeException(e);
	}

	private long getCollectionCreated(String key) {
		Long rtn = collectionsCreated.get(key);
		if (rtn != null) return rtn.longValue();
		return 0L;
	}

	private String fullKey(Key k) {

		return (k.bucket != null ? k.bucket : this.bucketName) + "/" + (k.scope != null ? k.scope : this.scopeName) + "/"
				+ (k.collection != null ? k.collection : this.collectionName) + "/" + k.key;
	}

	private Key toKey(String key) throws CouchbaseException {
		// <bucket>/<scope>/<collection>/<key>

		// TODO use core method for this
		List<String> list = new ArrayList<>();
		int start = 0, index;
		int count = 100;
		while (((index = key.indexOf('/', start)) != -1) && --count > 0) {
			list.add(key.substring(start, index).trim());
			start = index + 1;
		}

		if (start == key.length()) throw new CouchbaseException("key [" + key + "] is invalid"); // TODO better message
		list.add(key.substring(start).trim());
		if (list.size() > 4) throw new CouchbaseException("key [" + key + "] is invalid, follow this pattern [ <bucket>/<scope>/<collection>/<key>]");

		if (!hasBucket) {
			if (list.size() < 4) throw new CouchbaseException("key [" + key
					+ "] is invalid, you have not defined a bucket, scope or collection for this couchbase cache, so this has to be part of the key, following this pattern [ <bucket>/<scope>/<collection>/<key>]");
		}
		else if (!hasScope) {
			if (list.size() < 3) throw new CouchbaseException("key [" + key
					+ "] is invalid, you have not defined a scope or collection for this couchbase cache, so this has to be part of the key, following this pattern [ <scope>/<collection>/<key>]");
		}
		else if (!hasCollection) {
			if (list.size() < 2) throw new CouchbaseException("key [" + key
					+ "] is invalid, you have not defined a collection for this couchbase cache, so this has to be part of the key, following this pattern [ <collection>/<key>]");
		}
		return new Key(list, key, bucketName, scopeName, collectionName);
	}

	private static class Key {

		private String key;
		private String bucket;
		private String scope;
		private String collection;
		private String raw;

		public Key(String key, String bucketName, String scopeName, String collectionName) {
			this.bucket = bucketName;
			this.scope = scopeName;
			this.collection = collectionName;
			this.key = key;
			this.raw = key;
		}

		public Key(List<String> list, String raw, String bucketName, String scopeName, String collectionName) {
			this.raw = raw;
			this.bucket = bucketName;
			this.scope = scopeName;
			this.collection = collectionName;
			if (list.size() == 4) {
				this.bucket = list.get(0).trim();
				this.scope = list.get(1).trim();
				this.collection = list.get(2).trim();
				this.key = list.get(3).trim();
			}
			else if (list.size() == 3) {
				this.scope = list.get(0).trim();
				this.collection = list.get(1).trim();
				this.key = list.get(2).trim();
			}
			else if (list.size() == 2) {
				this.collection = list.get(0).trim();
				this.key = list.get(1).trim();
			}
			else if (list.size() == 1) {
				this.key = list.get(0).trim();
			}

			if (Util.isEmpty(raw)) {
				this.raw = "";
				if (!Util.isEmpty(this.bucket)) {
					this.raw += this.bucket + "/";
				}
				if (!Util.isEmpty(this.scope)) {
					this.raw += this.scope + "/";
				}
				if (!Util.isEmpty(this.collection)) {
					this.raw += this.collection + "/";
				}
				if (!Util.isEmpty(this.key)) {
					this.raw += this.key;
				}
			}
		}

		public String b() {
			return "" + this.bucket; // TODO if null set default values
		}

		public String bs() {
			return this.bucket + "/" + this.scope;// TODO if null set default values
		}

		public String bsc() {
			return this.bucket + "/" + this.scope + "/" + this.collection;// TODO if null set default values
		}

		@Override
		public String toString() {
			return this.raw;
		}

	}

	private void setAndValidate() throws CouchbaseException {

		hasBucket = !Util.isEmpty(bucketName, true);
		hasScope = !Util.isEmpty(scopeName, true);
		hasCollection = !Util.isEmpty(collectionName, true);

		if (!hasBucket) {
			if (hasScope) throw new CouchbaseException("invalid configuration, you cannot not define a bucket but a scope");
			if (hasCollection) throw new CouchbaseException("invalid configuration, you cannot not define a bucket but a collection");
		}
		else if (!hasScope) {
			if (hasCollection) throw new CouchbaseException("invalid configuration, you cannot not define a scope but a collection");
		}
	}
}
