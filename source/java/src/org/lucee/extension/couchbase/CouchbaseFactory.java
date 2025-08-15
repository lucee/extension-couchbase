package org.lucee.extension.couchbase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lucee.loader.util.Util;

public class CouchbaseFactory {

	private static Map<String, Couchbase> instances = new ConcurrentHashMap<>();
	private static Map<String, Object> tokens = new ConcurrentHashMap<>();

	public static Couchbase getInstance(String connectionString, String username, String password, String bucketName, String scope, String collection, short transcoder,
			long defaultExpire, long connectionTimeout, boolean createIfNecessaryBucket, boolean createIfNecessaryScope, boolean createIfNecessaryCollection)
			throws CouchbaseException {
		// validate connection string
		if (Util.isEmpty(connectionString, true)) throw new CouchbaseException("connection string is empty");
		// TODO if (!connectionString.startsWith("couchbases://"))

		// TODO validate bucket name,scopes and rules, what rules aply?
		String key = new StringBuilder(connectionString).append(':').append(username).append(':').append(password).append(':').append(bucketName).append(':').append(scope)
				.append(':').append(collection).append(':').append(transcoder).append(':').append(defaultExpire).append(':').append(connectionTimeout).append(':')
				.append(createIfNecessaryBucket).append(':').append(createIfNecessaryScope).append(':').append(createIfNecessaryCollection).toString();
		Couchbase cb = instances.get(key);
		if (cb == null) {
			synchronized (getToken(key)) {
				cb = instances.get(key);
				if (cb == null) {
					instances.put(key, cb = new Couchbase(connectionString, username, password, bucketName, scope, collection, transcoder, defaultExpire, connectionTimeout,
							createIfNecessaryBucket, createIfNecessaryScope, createIfNecessaryCollection));
				}
			}
		}
		return cb;
	}

	public static Object getToken(String key) {
		Object newLock = new Object();
		Object lock = tokens.putIfAbsent(key, newLock);
		if (lock == null) {
			lock = newLock;
		}
		return lock;
	}
}
