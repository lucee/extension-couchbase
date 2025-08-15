package org.lucee.extension.couchbase.coder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.op.Castable;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.ObjectWrap;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;

public class JSON {

	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static final String IK_STORAGEVALUE_KEY = "iksvk";

	public static JsonObject toJsonObject(Object o, JsonObject defaultValue) {
		try {
			if (o instanceof Struct) return _toJsonObjectStruct((Struct) o, new HashSet<>());
			if (o instanceof Map<?, ?>) return _toJsonObjectMap((Map<?, ?>) o, new HashSet<>());
		}
		catch (Exception e) {
		}
		return defaultValue;
	}

	public static JsonObject toJsonObject(Object o) throws IOException {

		if (o instanceof Struct) return _toJsonObjectStruct((Struct) o, new HashSet<>());
		if (o instanceof Map<?, ?>) return _toJsonObjectMap((Map<?, ?>) o, new HashSet<>());
		if (o == null) throw new IOException("cannot convert null value to a JSON object");
		throw new IOException("cannot convert [" + o.getClass().getName() + "] to a JSON object, object must be a struct or a map");
	}

	private static Object toJsonValue(Object o, Set<Object> inside) throws IOException {
		// simple values
		if (o == null) return JsonValue.NULL;
		if (o instanceof CharSequence) return o.toString();
		if (o instanceof Boolean) return o;
		if (o instanceof Number) {
			if (o instanceof Double) return o;
			if (o instanceof Integer) return o;
			if (o instanceof Long) return o;
			if (o instanceof BigDecimal) return ((BigDecimal) o).doubleValue();
			return ((Number) o).doubleValue();
		}
		if (o instanceof Date) return o; // TODO format
		if (o instanceof byte[]) return o;

		// complex value

		if (o instanceof Collection) {
			try {

				if (inside.contains(o))
					throw new IOException("object cannot be serialized, betcause it has a internal relation to itself, one object is pointing to itself of one of its parents!");
				inside.add(o);
				return _toJsonValue(((Collection) o), inside);
			}
			finally {
				inside.remove(o);
			}
		}
		if (o instanceof Map<?, ?>) {
			try {

				if (inside.contains(o))
					throw new IOException("object cannot be serialized, betcause it has a internal relation to itself, one object is pointing to itself of one of its parents!");
				inside.add(o);
				return _toJsonObjectMap((Map<?, ?>) o, inside);
			}
			finally {
				inside.remove(o);
			}
		}
		if (o instanceof java.util.Collection<?>) {
			try {

				if (inside.contains(o))
					throw new IOException("object cannot be serialized, betcause it has a internal relation to itself, one object is pointing to itself of one of its parents!");
				inside.add(o);
				return _toJsonValue(((java.util.Collection<?>) o), inside);
			}
			finally {
				inside.remove(o);
			}
		}

		if (o instanceof ObjectWrap) {
			try {
				return toJsonValue(((ObjectWrap) o).getEmbededObject(), inside);
			}
			catch (PageException e) {
				throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
			}

		}
		if (o instanceof Castable) {
			try {
				return toJsonValue(((Castable) o).castToString(), inside);
			}
			catch (PageException e) {
				throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
			}
		}

		/*
		 * if (o instanceof Serializable) { return toJsonValue(Coder.compress(o), inside); }
		 */

		// Decision dec = CFMLEngineFactory.getInstance().getDecisionUtil();

		throw new IOException("type [" + o.getClass().getName() + "] cannot be converted to JSON yet!");
	}

	private static JsonValue _toJsonValue(java.util.Collection<?> coll, Set<Object> inside) throws IOException {
		Iterator<?> it = coll.iterator();
		JsonArray arr = JsonArray.create();
		while (it.hasNext()) {
			arr.add(toJsonValue(it.next(), inside));
		}
		return arr;
	}

	private static JsonValue _toJsonValue(Collection coll, Set<Object> inside) throws IOException {
		if (coll instanceof Struct) return _toJsonObjectStruct((Struct) coll, inside);
		if (coll instanceof Map<?, ?>) return _toJsonObjectMap((Map<?, ?>) coll, inside);

		Iterator<?> it = coll.getIterator();
		JsonArray arr = JsonArray.create();
		while (it.hasNext()) {
			arr.add(toJsonValue(it.next(), inside));
		}
		return arr;
	}

	private static JsonObject _toJsonObjectStruct(Struct sct, Set<Object> inside) throws IOException {
		Iterator<Entry<Key, Object>> it = sct.entryIterator();
		Entry<Key, Object> e;
		JsonObject doc = JsonObject.create();
		while (it.hasNext()) {
			e = it.next();
			doc.put(e.getKey().toString(), toJsonValue(e.getValue(), inside));
		}
		return doc;
	}

	private static JsonObject _toJsonObjectMap(Map<?, ?> map, Set<Object> inside) throws IOException {
		Iterator<?> it = map.entrySet().iterator();
		Entry<?, ?> e;
		JsonObject doc = JsonObject.create();
		while (it.hasNext()) {
			e = (Entry<?, ?>) it.next();
			doc.put(e.getKey().toString(), toJsonValue(e.getValue(), inside)); // TODO do caster.toString here
		}
		return doc;
	}

	private static Object evaluate(ClassLoader cl, byte[] data) throws IOException {
		if (data == null) return null;

		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStreamImpl(cl, bais);
			return ois.readObject();
		}
		catch (ClassNotFoundException e) {
			throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
		finally {
			Util.closeEL(ois);
		}
	}

	private static class ObjectInputStreamImpl extends ObjectInputStream {

		private ClassLoader cl;

		public ObjectInputStreamImpl(ClassLoader cl, InputStream in) throws IOException {
			super(in);
			this.cl = cl;
		}

		@Override
		protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
			if (cl == null) return super.resolveClass(desc);

			String name = desc.getName();
			try {
				return Class.forName(name, false, cl);
			}
			catch (ClassNotFoundException ex) {
				return super.resolveClass(desc);
			}
		}

	}

	public static Object toCFMLObject(JsonObject jo) throws IOException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Set<Entry<String, Object>> set = jo.toMap().entrySet();
		Struct sct = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		try {
			for (Entry<String, Object> e: set) {
				sct.put(e.getKey(), _toCFMLObject(eng, e.getValue()));
			}
		}
		catch (PageException pe) {
			throw eng.getExceptionUtil().toIOException(pe);
		}
		return sct;
	}

	private static Object _toCFMLObject(CFMLEngine eng, Object o) throws PageException {
		if (o instanceof Map) {
			Struct sct = eng.getCreationUtil().createStruct();
			Cast util = eng.getCastUtil();
			Entry e;
			Iterator it = ((Map) o).entrySet().iterator();
			while (it.hasNext()) {
				e = (Entry) it.next();
				sct.set(util.toString(e.getKey()), _toCFMLObject(eng, e.getValue()));
			}
			return sct;
		}
		else if (o instanceof List) {
			Array arr = eng.getCreationUtil().createArray();
			Cast util = eng.getCastUtil();
			Iterator it = ((List) o).iterator();
			Object v;
			while (it.hasNext()) {
				v = it.next();
				arr.append(_toCFMLObject(eng, v));
			}
			return arr;
		}
		// TODO more
		return o;
	}

}
