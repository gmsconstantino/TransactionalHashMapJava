package fct.thesis.databaseNMSI_Array;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Constantino Gomes on 15/07/15.
 */
public class CMapSnapshotsImpl<T extends Transaction> implements SnapshotsIface<T> {

    ConcurrentHashMap<T,Long> snapshots = new ConcurrentHashMap<>();

    @Override
    public Long get(T t) {
        return snapshots.get(t);
    }

    @Override
    public void put(T t, long v) {
        snapshots.put(t,v);
    }

    @Override
    public void remove(T t) {
        snapshots.remove(t);
    }

    @Override
    public Iterable<Map.Entry<T, Long>> entrySet() {
        return snapshots.entrySet();
    }

    @Override
    public Collection<? extends Transaction> keySet() {
        return snapshots.keySet();
    }
}
