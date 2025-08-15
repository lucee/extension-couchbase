package org.lucee.extension.couchbase;

import java.io.IOException;
import java.util.Date;

import org.lucee.extension.couchbase.coder.Coder;
import org.lucee.extension.couchbase.coder.JSON;
import org.lucee.extension.couchbase.util.print;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.kv.GetResult;

import lucee.commons.io.cache.CacheEntry;
import lucee.runtime.type.Struct;

public class CouchbaseEntry implements CacheEntry {

	private Couchbase couchbase;
	private final GetResult result;
	private final String key;
	private final short transcoder;
	private Object value;

	public CouchbaseEntry(Couchbase couchbase, GetResult result, String key, short transcoder) throws IOException {
		this.couchbase = couchbase;
		this.result = result;
		this.key = key;
		this.transcoder = transcoder;

		if (transcoder == Couchbase.TRANSCODER_BINARY) {
			try {
				value = Coder.evaluate(null, result.contentAsBytes());
			}
			catch (InvalidArgumentException iae) {
				print.e(iae);
				value = result.contentAsObject();
			}
		}
		else {
			value = JSON.toCFMLObject(result.contentAsObject());
		}
	}

	@Override
	public Date created() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct getCustomInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public int hitCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long idleTimeSpan() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Date lastHit() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date lastModified() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long liveTimeSpan() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
