package fct.thesis.databaseBlotter;

import fct.thesis.database.BufferObjectDb;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionTimeoutException;
import fct.thesis.database2PL.Config;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends fct.thesis.database.Transaction<K,V> {

    static AtomicLong identifier = new AtomicLong(-1L);

    Set<Transaction> aggStarted;
    protected Map<K, BufferObjectDb<K,V>> writeSet;

    public Transaction(fct.thesis.database.Database db) {
        super(db);
    }

    protected void init(){
        super.init();
        aggStarted = new HashSet<>();
        writeSet = new HashMap<>();
        id = Transaction.identifier.incrementAndGet();
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

        boolean lock = false;

        Long v = obj.getVersionForTransaction(id);
        if (v==null){
            obj.lock_read();
            lock = true;
            v = obj.getLastVersion();
            obj.putSnapshot(this, v);
        }

        V r = obj.getValueVersion(v, aggStarted);

        if (lock)
            obj.unlock_read();

        return r;
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

        BufferObjectDb<K,V> buffer = new BufferObjectDb(key, value);
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

        Set<ObjectBlotterDb<K,V>> lockObjects = new HashSet<ObjectBlotterDb<K,V>>();

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {

            ObjectBlotterDb<K,V> objectDb = (ObjectBlotterDb) getKeyDatabase(buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                ObjectBlotterDb<K,V> obj = new ObjectBlotterDb<K,V>(); // Thread tem o lock do objecto
                obj.lock_write();

                ObjectBlotterDb<K,V> objdb = (ObjectBlotterDb) putIfAbsent(buffer.getKey(), obj);
                if (objdb != null) {
                    obj = null;
                    objectDb = objdb;
                }else {
                    objectDb = obj;
                }
            }


            if (objectDb.try_lock_write_for(Config.TIMEOUT,Config.TIMEOUT_UNIT)) {
                lockObjects.add(objectDb);
                buffer.setObjectDb(objectDb); // Set reference to Object, para que no ciclo seguindo
                                              // nao seja necessario mais uma pesquisa no hashmap

                Long v = objectDb.getVersionForTransaction(id);
                if(v != null && v < objectDb.getLastVersion()){
                    abortVersions(lockObjects);
                    return false;
                } else {
                    // Line 22
                    aggStarted.addAll(objectDb.snapshots.keySet());
                }
            } else {
                abortVersions(lockObjects);
            }
        }

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
            ObjectBlotterDb<K, V> objectDb = (ObjectBlotterDb) buffer.getObjectDb();

            for (Transaction tid : aggStarted){
                if (tid.isActive() && objectDb.snapshots.get(tid) == null){
                    objectDb.putSnapshot(this, objectDb.getLastVersion());
                }
            }

            objectDb.setValue(buffer.getValue());
        }

        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortVersions(Set<ObjectBlotterDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectBlotterDb<K,V>> set){
        Iterator<ObjectBlotterDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectBlotterDb<K,V> objectDb = it_locks.next();
            objectDb.unlock_write();
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
