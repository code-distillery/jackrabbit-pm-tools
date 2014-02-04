package net.distilledcode.jackrabbit.pm.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<A, B> extends LinkedHashMap<A, B> {

    private final int maxEntries;

    public LRUCache(final int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        this.maxEntries = maxEntries;
    }

    @Override
    public B get(Object o) {
        return super.get(o);
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
        return super.size() > maxEntries;
    }
}
