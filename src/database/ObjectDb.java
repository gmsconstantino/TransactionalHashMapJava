package database;

import structures.RwLock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by gomes on 26/02/15.
 */
public class ObjectDb<K,V> {

    K key;
    V value;

    RwLock rwlock;
//    ConcurrentHashMap<>

    ObjectDb(K key){
        rwlock = new RwLock();
        this.key = key;
    }

}
