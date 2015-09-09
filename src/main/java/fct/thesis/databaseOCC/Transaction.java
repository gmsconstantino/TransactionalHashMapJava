package fct.thesis.databaseOCC;

import fct.thesis.database.*;
import fct.thesis.database2PL.Config;
import fct.thesis.structures.P;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends TransactionAbst<K,V> {

    protected Map<P<Integer,K>, BufferDb<K,V>> readSet;
    protected Map<P<Integer,K>, BufferDb<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new TreeMap<>();
        writeSet = new TreeMap<>();
    }

    @Override
    public V get(int table, K k) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        P key = new P<>(table,k);
        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue;
        long versionObj;

        ObjectLockOCC<K,V> obj = (ObjectLockOCC) getKeyDatabase(table, k);
        if (obj == null || obj.getVersion() == -1)
            return null;

        obj.lock_read();
        returnValue = obj.getValue();
        versionObj = obj.getVersion();
        obj.unlock_read();

        if (readSet.containsKey(key)){
            if (versionObj != readSet.get(key).getVersion()) {
                abort();
                throw new TransactionAbortException("GET: Transaction Abort " + getId() + ": " + Thread.currentThread().getName() + " - Version change - key:" + key);
            }
        } else {
            addObjectDbToReadBuffer(key, new BufferObjectDb(table, k, returnValue, versionObj, obj));
        }

        return returnValue;
    }

    @Override
    public void put(int table, K k, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        P key = new P<>(table,k);
        if(writeSet.containsKey(key)){
            writeSet.get(key).setValue(value); // set new value on buffer
            return;
        }

        ObjectLockOCC<K,V> obj = (ObjectLockOCC) getKeyDatabase(table, k);
        if (obj == null) {
            obj = new ObjectLockOCC<K,V>(null); // A thread fica com o write lock
            ObjectLockOCC<K,V> objdb = (ObjectLockOCC) putIfAbsent(table, k, obj);

            if (objdb != null)
                obj = objdb;
        }

        // o objecto esta na base de dados
        BufferObjectDb<K,V> buffer = new BufferObjectDb(table, k, value, obj.getVersion(), obj);
        addObjectDbToWriteBuffer(key, buffer);
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        Set<ObjectLockDb<V>> lockObjects = new HashSet<>();

        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectLockOCC<K,V> objectDb = (ObjectLockOCC) buffer.getObjectDb();

            objectDb.lock_write();
            lockObjects.add(objectDb);

            if (buffer.getVersion() == objectDb.getVersion()) {
                continue;
            } else {
                abortVersions(lockObjects);
                return false;
            }
        }

        // Validate Read Set
        for (BufferDb<K,V> buffer : readSet.values()){ // BufferObject
            ObjectLockOCC<K,V> objectDb = (ObjectLockOCC) buffer.getObjectDb();
            if(objectDb.try_lock_read_for(Config.TIMEOUT, Config.TIMEOUT_UNIT)){

                if (buffer.getVersion() == objectDb.getVersion()) {
                    objectDb.unlock_read();
                    continue;
                } else {
                    objectDb.unlock_read();
                    abortVersions(lockObjects);
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
            }
        }

//        commitId = Database.timestamp.getAndIncrement();

        // Escrita
        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectLockOCC<K,V> objectDb = (ObjectLockOCC) buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
            objectDb.unlock_write();
        }

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Set<ObjectLockDb<V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("COMMIT: Transaction " + getId() +": "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectLockDb<V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("COMMIT: Transaction Abort " + getId() +": "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectLockDb<V>> set){
        Iterator<ObjectLockDb<V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectLockDb<V> objectDb = it_locks.next();
            objectDb.unlock_write();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
    }

    void addObjectDbToReadBuffer(P key, BufferDb<K,V> objectDb){
        readSet.put(key, objectDb);
    }

    void addObjectDbToWriteBuffer(P key, BufferDb objectDb){
        writeSet.put(key, objectDb);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", readSet=" + readSet +
                ", writeSet=" + writeSet +
                ", isActive=" + isActive +
                ", success=" + success +
                '}';
    }

}
