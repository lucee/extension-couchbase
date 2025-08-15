package org.lucee.extension.couchbase;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.lucee.extension.couchbase.util.print;

public class Teat {
	// Update these variables to point to your Couchbase Capella instance and credentials.
	static String connectionString = "couchbases://cb.fzq54xrzioqpnlk.cloud.couchbase.com";
	static String username = "admin";
	static String password = "redBat73!";
	static String bucketName = "test1";
	static String scopeName = "test1";
	static String collectionName = "test2";

	public static void main(String... args) throws IOException, InterruptedException {
		String key = "susi2";
		Couchbase cb = CouchbaseFactory.getInstance(connectionString, username, password, bucketName, scopeName, collectionName, Couchbase.TRANSCODER_JSON, 0, 10000, false, false,
				false);
		/*
		 * print.e("+++++++++++++++++++++++++++++++++"); print.e(cb.existsBucket("test1"));
		 * print.e(cb.listBuckets());
		 * 
		 * print.e("+++++++++++++++++++++++++++++++++"); print.e(cb.existsScope("test1", "test1"));
		 * print.e(cb.listScopes("test1"));
		 * 
		 * print.e("+++++++++++++++++++++++++++++++++"); print.e(cb.existsCollection("test1", "test1",
		 * "test2")); print.e(cb.listCollections("test1", "test1"));
		 * print.e("+++++++++++++++++++++++++++++++++");
		 */
		print.e("--- " + cb.contains(key));
		// if (true) return;

		// print.e(cb.getCacheEntry("my-document").getValue());

		long start = System.currentTimeMillis();
		while (true) {

			HashMap m = new HashMap<>();
			m.put("susi", "Sorglos");

			print.e("--- " + new Date() + " - " + ((System.currentTimeMillis() - start) / 1000) + " ---" + cb.contains(key));
			cb.put(key, m, 100000l, null);
			print.e("K:" + cb.getCacheEntry(key).getKey());
			print.e("v:" + cb.getCacheEntry(key).getValue());
			print.e("contains:" + cb.contains(key));
			print.e(cb.meta(key));

			// print.e(cb.keys());

			Thread.sleep(5000);

		}
	}

}
