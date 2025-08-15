package org.lucee.extension.couchbase.util;

import java.io.Serializable;

import org.lucee.extension.couchbase.Couchbase;

import lucee.loader.util.Util;

public class CouchbaseUtil {

	public static Object toJsonObject(Object value) {
		// TODO
		return value;
	}

	public static Serializable toSerializable(Object val) {
		return (Serializable) val;// TODO make much better
	}

	public static String escape(String criteria) {
		// TODO escape `
		return criteria;
	}

	public static String toSQL(String criteria) {
		// TODO escape "
		return criteria.replace('*', '%');
	}

	public static short toTranscoder(String transcoder, short defaultValue) {
		if (Util.isEmpty(transcoder, true)) return defaultValue;
		transcoder = transcoder.trim().toLowerCase();
		if ("object".equals(transcoder)) return Couchbase.TRANSCODER_BINARY;
		if ("binary".equals(transcoder)) return Couchbase.TRANSCODER_BINARY;
		if ("json".equals(transcoder)) return Couchbase.TRANSCODER_JSON;
		return defaultValue;
	}

}
