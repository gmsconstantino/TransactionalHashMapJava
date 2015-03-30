package databaseBlotter;

import database.ObjectDb;
import databaseOCC.ObjectVersionLockDB;
import databaseOCC.ObjectVersionLockDBImpl;
import databaseOCCMulti.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 23/03/15.
 */
public class ObjectBlotterDbImpl<K,V> implements ObjectBlotterDb<K,V> {

    public AtomicInteger prewrite;
    public V nextValue;

    AtomicLong version;

    LinkedList<Pair<Long, ObjectVersionLockDB<K,V>>> objects;
    ConcurrentHashMap<Long, Long> snapshots; //Devia ter ttl

    public ObjectBlotterDbImpl(){
        prewrite = new AtomicInteger(0);
        version = new AtomicLong(-1L);

        objects = new LinkedList<>();
        snapshots = new ConcurrentHashMap<>();
    }

    public Long getLastVersion() {
        return version.get();
    }

    public Pair<V,List<Long>> getValueTransaction(long id) {
        Long lVersion = snapshots.get(id);

        if (lVersion==null){
            while (!prewrite.compareAndSet(0,1)){}
            snapshots.put(id, version.get());
            lVersion = version.get();
            prewrite.set(0);
        }

        Pair<V,List<Long>> p = new Pair<V,List<Long>>();
        p.s = new LinkedList<>();

        for(Pair<Long, ObjectVersionLockDB<K,V>> pair : objects){
            if(p.f==null && pair.f <= lVersion){
                p.f = pair.s.getValue();
                break;
            }
        }

        if (p.f != null){
            for (Long tid : snapshots.keySet()){
                if (snapshots.get(tid) < lVersion)
                    p.s.add(tid);
            }
            return p;
        }

        return null;
    }

    @Override
    public boolean preWrite(long id, V value) {

        while (!prewrite.compareAndSet(0,1)){}

        if(snapshots.get(id) != null && snapshots.get(id) < version.get()){
            prewrite.set(0);
            return false;
        }

        nextValue = value;
        return true;
    }

    @Override
    public void unPreWrite() {
        prewrite.set(0);
    }

    @Override
    public void write(Set<Long> agg) {

        for (Long tid : agg){
            if (!snapshots.contains(tid))
                snapshots.put(tid, version.get());
        }

        Long v = version.incrementAndGet();

        ObjectVersionLockDBImpl<K,V> obj = new ObjectVersionLockDBImpl<K,V>(nextValue);
        objects.addFirst(new Pair(v, obj));
        obj.unlock_write();
    }

    @Override
    public ObjectDb getObjectDb() {
        return this;
    }

    @Override
    public V getValue() {
        return null;
    }

    @Override
    public String toString() {
        return "ObjectMultiVersionDB{" +
                "last_version=" + version.get() +
                ", objects=" + objects +
                '}';
    }

}