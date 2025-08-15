package org.lucee.extension.couchbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.lucee.extension.couchbase.util.CacheUtil;

import lucee.commons.io.cache.Cache;
import lucee.commons.io.cache.CacheEntry;
import lucee.commons.io.cache.CacheEntryFilter;
import lucee.commons.io.cache.CacheKeyFilter;
import lucee.commons.io.cache.exp.CacheException;

/**
 * this class handles all action that oare independent of the cache type used
 */
public abstract class CacheSupport implements Cache {

	public static final Charset UTF8 = Charset.forName("UTF-8");

	@Override
	public List<String> keys(CacheEntryFilter filter) throws IOException {
		boolean all = CacheUtil.allowAll(filter);
		List<String> keys = keys();
		List<String> list = new ArrayList<String>();
		Iterator<String> it = keys.iterator();
		String key;
		CacheEntry entry;
		while (it.hasNext()) {
			key = it.next();
			entry = getQuiet(key, null);
			if (entry != null && (all || filter.accept(entry))) list.add(key);
		}
		return list;
	}

	@Override
	public List<CacheEntry> entries() throws IOException {
		// TODO faster way to do this
		return _entries(keys());
	}

	@Override
	public List<CacheEntry> entries(CacheKeyFilter filter) throws IOException {
		// TODO faster way to do this
		return _entries(keys(filter));
	}

	private List<CacheEntry> _entries(List<String> keys) throws IOException {
		// TODO faster way to do this

		List<CacheEntry> entries = new ArrayList<CacheEntry>();
		if (keys != null) for (String k: keys) {
			entries.add(getCacheEntry(k));
		}
		return entries;
	}

	public int clear() throws IOException {
		// TODO faster way to do this
		return _remove(keys());
	}

	@Override
	public int remove(CacheKeyFilter filter) throws IOException {
		// TODO faster way to do this
		return _remove(keys(filter));
	}

	private int _remove(List<String> keys) throws IOException {
		// TODO faster way to do this
		int count = 0;
		if (keys != null) for (String k: keys) {
			remove(k);
			count++;
		}
		return count;
	}

	@Override
	public List<CacheEntry> entries(CacheEntryFilter filter) throws IOException {
		List<CacheEntry> entries = entries();
		List<CacheEntry> list = new ArrayList<CacheEntry>();
		Iterator<CacheEntry> it = entries.iterator();
		CacheEntry entry;
		while (it.hasNext()) {
			entry = it.next();
			if (entry != null && (filter == null || filter.accept(entry))) {
				list.add(entry);
			}
		}
		return list;
	}

	@Override
	public List<Object> values() throws IOException {
		// TODO faster way to do this
		return _values(keys());
	}

	@Override
	public List<Object> values(CacheKeyFilter filter) throws IOException {
		// TODO faster way to do this
		return _values(keys(filter));
	}

	private List<Object> _values(List<String> keys) throws IOException {
		List<Object> values = new ArrayList<Object>();
		if (values != null) for (String k: keys) {
			values.add(getValue(k));
		}
		return values;
	}

	// there was the wrong generic type defined in the older interface, because of that we do not define
	// a generic type at all here, just to be sure
	@Override
	public List values(CacheEntryFilter filter) throws IOException {
		if (CacheUtil.allowAll(filter)) return values();

		List<String> keys = keys();
		List<Object> list = new ArrayList<Object>();
		Iterator<String> it = keys.iterator();
		String key;
		CacheEntry entry;
		while (it.hasNext()) {
			key = it.next();
			entry = getQuiet(key, null);
			if (entry != null && (filter == null || filter.accept(entry))) list.add(entry.getValue());
		}
		return list;
	}

	// there was the wrong generic type defined in the older interface, because of that we do not define
	// a generic type at all here, just to be sure

	@Override
	public int remove(CacheEntryFilter filter) throws IOException {
		if (CacheUtil.allowAll(filter)) return clear();

		List<String> keys = keys();
		int count = 0;
		Iterator<String> it = keys.iterator();
		String key;
		CacheEntry entry;
		while (it.hasNext()) {
			key = it.next();
			entry = getQuiet(key, null);
			if (entry != null && (filter == null || filter.accept(entry))) {
				remove(key);
				count++;
			}
		}
		return count;
	}

	@Override
	public Object getValue(String key) throws IOException {
		return getCacheEntry(key).getValue();
	}

	@Override
	public Object getValue(String key, Object defaultValue) {
		CacheEntry entry = getCacheEntry(key, null);
		if (entry == null) return defaultValue;
		return entry.getValue();
	}

	protected static boolean valid(CacheEntry entry) {
		if (entry == null) return false;
		long now = System.currentTimeMillis();
		if (entry.liveTimeSpan() > 0 && entry.liveTimeSpan() + getTime(entry.lastModified()) < now) {
			return false;
		}
		if (entry.idleTimeSpan() > 0 && entry.idleTimeSpan() + getTime(entry.lastHit()) < now) {
			return false;
		}
		return true;
	}

	private static long getTime(Date date) {
		return date == null ? 0 : date.getTime();
	}

	public CacheEntry getQuiet(String key) throws IOException {
		CacheEntry entry = getQuiet(key, null);
		if (entry == null) throw new CacheException("there is no valid cache entry with key [" + key + "]");
		return entry;
	}

	public abstract CacheEntry getQuiet(String key, CacheEntry defaultValue);

	// CachePro interface @Override
}