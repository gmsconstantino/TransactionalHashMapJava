package fct.thesis.structures;

import java.util.Map;

/**
 * Created by gomes on 26/02/15.
 */
public final class MapEntry<K, V> implements Map.Entry<K, V> {
    public K f;
    public V s;

    public MapEntry() {
    }

    public MapEntry(K f, V value) {
        this.f = f;
        this.s = value;
    }

    @Override
    public K getKey() {
        return f;
    }

    @Override
    public V getValue() {
        return s;
    }

    @Override
    public V setValue(V value) {
        V old = this.s;
        this.s = value;
        return old;
    }
}