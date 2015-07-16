package fct.thesis.databaseNMSI;

import fct.thesis.database.BufferObjectDb;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionTimeoutException;
import pt.dct.util.P;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends fct.thesis.database.Transaction<K,V> {

    public Set<Transaction> aggStarted;
    protected Map<K, BufferObjectDb<K,V>> writeSet;

    public Transaction(fct.thesis.database.Database db) {
        super(db);
    }

    protected Transaction() {
        super(null);
        abort();
    }

    protected void init(){
        super.init();
        aggStarted = new HashSet<>();
        writeSet = new TreeMap<>();
        thread = Thread.currentThread();
        idxThread = (int) thread.getId() % SnapshotsIface.MAX_POS;
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        ObjectNMSIDb<K,V> obj = (ObjectNMSIDb) getKeyDatabase(key);
        if (obj == null || obj.getLastVersion() == -1)
            return null;

        Long v = obj.getVersionForTransaction(this);
        if (v==null){
            obj.lock_read();
            v = obj.getVersionForTransaction(this);
            if (v==null){
                v = obj.getLastVersion();
                obj.putSnapshot(this, v);
            }
            obj.unlock_read();
        }

        V r = obj.getValueVersion(v, aggStarted);

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

        Set<ObjectNMSIDb<K,V>> lockObjects = new HashSet<>();

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {

            ObjectNMSIDb<K,V> objectDb = (ObjectNMSIDb) getKeyDatabase(buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                ObjectNMSIDb<K,V> obj = new ObjectNMSIDb<K,V>(); // Thread tem o lock do objecto
                obj.lock_write();

                ObjectNMSIDb<K,V> objdb = (ObjectNMSIDb) putIfAbsent(buffer.getKey(), obj);
                if (objdb != null) {
                    objectDb = objdb;
                }else {
                    objectDb = obj;
                }
            }


            objectDb.lock_write();
            lockObjects.add(objectDb);
            buffer.setObjectDb(objectDb); // Set reference to Object, para que no ciclo seguindo
                                          // nao seja necessario mais uma pesquisa no hashmap

            Long v = objectDb.getVersionForTransaction(this);
            if(v != null && v < objectDb.getLastVersion()){
                abortVersions(lockObjects);
                return false;
            } else {
                // Line 22
                aggStarted.addAll(objectDb.snapshots.keySet());
            }
        }

        for (BufferObjectDb<K, V> buffer : writeSet.values()) {
            ObjectNMSIDb<K, V> objectDb = (ObjectNMSIDb) buffer.getObjectDb();
            for (Transaction tid : aggStarted){
                if (tid.isActive() && objectDb.snapshots.get(tid) == null){
                    objectDb.putSnapshot(tid, objectDb.getLastVersion());
                }
            }

            objectDb.setValue(buffer.getValue());
            objectDb.unlock_write();
        }

        isActive = false;
        success = true;
        return true;
    }

    protected void abortVersions(Set<ObjectNMSIDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    protected void unlockWrite_objects(Set<ObjectNMSIDb<K,V>> set){
        Iterator<ObjectNMSIDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectNMSIDb<K,V> objectDb = it_locks.next();
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
    public Collection getWriteSet() {
        return writeSet.values();
    }

    @Override
    public String toString() {
        return "Transaction@"+System.identityHashCode(this)+" {" +
                "id=" + id +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }
}
