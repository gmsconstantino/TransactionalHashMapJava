package structures;

import java.util.Map;

/**
 * Created by gomes on 26/02/15.
 */
public final class MapEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    public MapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }
}