package databaseBlotter;

import database.*;
import database2PL.BufferObjectDb;
import database2PL.Config;
import databaseOCC.BufferObjectVersionDB;
import databaseOCC.ObjectVersionLockDB;
import databaseOCCMulti.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends database.Transaction<K,V> {

    static AtomicInteger identifier = new AtomicInteger(0);

    Set<Long> aggStarted;
    protected Map<K, BufferObjectDb<K,V>> writeSet;

    private long id;

    public Transaction(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();
        aggStarted = new HashSet<>();
        writeSet = new HashMap<K, BufferObjectDb<K,V>>();
        id = Transaction.identifier.get();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        ObjectBlotterDb<K,V> obj = (ObjectBlotterDb) getKeyDatabase(key);
        if (obj == null || obj.getLastVersion() == -1)
            return null;

        Pair<V,List<Long>> r = obj.getValueTransaction(id);

        if (r!=null) {
            aggStarted.addAll(r.s);
            return r.f;
        }

        return null;
    }

    @Override
    public V get_to_update(K key) throws TransactionTimeoutException{
        return get(key);
    }

    @Override
    public void put(K key, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        if(writeSet.containsKey(key)){
            BufferObjectDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        ObjectBlotterDb<K,V> obj = (ObjectBlotterDb) getKeyDatabase(key);
        //Nao existe nenhuma
        if (obj == null) {
            obj = new ObjectBlotterDbImpl<>(); // A thread fica com o write lock
            ObjectBlotterDbImpl<K,V> objdb = (ObjectBlotterDbImpl) putIfAbsent(key, obj);

            if (objdb != null)
                obj = objdb;
        }

        BufferObjectDb<K,V> buffer = new BufferObjectDb(value ,obj);
        buffer.setValue(value);
        addObjectDbToWriteBuffer(key, buffer);
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        if (writeSet.size() == 0){
            isActive = false;
            success = true;
            return true;
        }

        Set<ObjectBlotterDbImpl<K,V>> lockObjects = new HashSet<>();

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
            ObjectBlotterDbImpl<K, V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();

            if (objectDb.preWrite(id, buffer.getValue())) {
                lockObjects.add(objectDb);
            } else {
                abortVersions(lockObjects);
            }
        }

        commitId = Transaction.identifier.getAndIncrement();

        // Escrita
        for (ObjectDb<K,V> buffer : writeSet.values()){
            ObjectBlotterDbImpl<K,V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();
            aggStarted.addAll(objectDb.snapshots.keySet());
        }

        for (ObjectDb<K,V> buffer : writeSet.values()) {
            ObjectBlotterDbImpl<K,V> objectDb = (ObjectBlotterDbImpl) buffer.getObjectDb();
            objectDb.write(aggStarted);
        }

            unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortVersions(Set<ObjectBlotterDbImpl<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectBlotterDbImpl<K,V>> set){
        Iterator<ObjectBlotterDbImpl<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectBlotterDbImpl<K,V> objectDb = it_locks.next();
            objectDb.unPreWrite();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
    }

    private void addObjectDbToWriteBuffer(K key, BufferObjectDb objectDb){
        writeSet.put(key, objectDb);
    }


    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", writeSet=" + writeSet +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }

}
