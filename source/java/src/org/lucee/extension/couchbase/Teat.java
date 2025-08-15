package org.lucee.extension.couchbase;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.lucee.extension.couchbase.util.print;

public class Teat {
	// Update these variables to point to your Couchbase Capella instance and credentials.
	static String connectionString = "couchbases://cb.scnzb8nyua1-z6i.cloud.couchbase.com";
	static String username = "admin";
	static String password = "redBat73!";
	static String bucketName = "session";
	static String scopeName = "storage5";
	static String collectionName = "data5";

	public static void main(String... args) throws IOException, InterruptedException {
		String key = "susi2";
		Couchbase cb = CouchbaseFactory.getInstance(connectionString, username, password, bucketName, scopeName, collectionName, Couchbase.TRANSCODER_BINARY, 0, 10000, true, true,
				true);

		cb.put("susis", "Susnne", 10000L, 100000L);

		if (true) return;
		print.e("+++++++++++++++++++++++++++++++++");
		print.e(cb.existsBucket("session"));
		print.e(cb.listBuckets());
		print.e("+++++++++++++++++++++++++++++++++");
		print.e(cb.existsScope("session", "storage"));
		print.e(cb.listScopes("session"));

		print.e("+++++++++++++++++++++++++++++++++");
		print.e(cb.existsCollection("session", "storage", "data"));
		print.e(cb.listCollections("session", "storage"));
		print.e("+++++++++++++++++++++++++++++++++");

		if (true) return;

		print.e("--- " + cb.contains(key));
		// if (true) return;

		print.e(cb.getCacheEntry("ssmy-document").getValue());

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
