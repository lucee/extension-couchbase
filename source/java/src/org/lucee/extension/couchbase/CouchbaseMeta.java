package org.lucee.extension.couchbase;

public class CouchbaseMeta {

	public final String id;
	public final int flags;
	public final long expiration;
	public final long cas;
	public final String type;

	public CouchbaseMeta(String id, int flags, long expiration, long cas, String type) {
		this.id = id;
		this.flags = flags;
		this.expiration = expiration;
		this.cas = cas;
		this.type = type;
	}

	@Override
	public String toString() {
		return "id:" + id + ";flags:" + flags + ";expiration:" + expiration + ";cas:" + cas + ";type:" + type;
	}

}
