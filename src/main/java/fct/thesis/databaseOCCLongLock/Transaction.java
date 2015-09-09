package fct.thesis.databaseOCCLongLock;

import fct.thesis.database.*;
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

        ObjectLockOCCLongLock<K,V> obj = (ObjectLockOCCLongLock) getKeyDatabase(table, k);
        if (obj == null || obj.getVersion() == -1)
            return null;

        if (readSet.containsKey(key)){
            return readSet.get(key).getValue();
        } else {
            returnValue = obj.getValue();
            versionObj = obj.getVersion();
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

        ObjectLockOCCLongLock<K,V> obj = (ObjectLockOCCLongLock) getKeyDatabase(table, k);
        if (obj == null) {
            obj = new ObjectLockOCCLongLock<K,V>(null); // A thread fica com o write lock
            ObjectLockOCCLongLock<K,V> objdb = (ObjectLockOCCLongLock) putIfAbsent(table, k, obj);

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

        long version;
        long thread_id = Thread.currentThread().getId();
        Set<ObjectLockDb<V>> lockObjects = new HashSet<ObjectLockDb<V>>();

        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectLockOCCLongLock<K,V> objectDb = (ObjectLockOCCLongLock) buffer.getObjectDb();

//            objectDb.lock_write();
            do {
                version = objectDb.getVersion();
            }while (!objectDb.compareAndSet(LockHelper.unlock(version), LockHelper.lock(version, thread_id)));

            lockObjects.add(objectDb);
            buffer.setVersion(objectDb.getVersion());

            if (LockHelper.getVersion(buffer.getVersion()) != LockHelper.getVersion(objectDb.getVersion())) {
                abortVersions(lockObjects);
                return false;
            }
        }

        // Validate Read Set
        for (BufferDb<K,V> buffer : readSet.values()){ // BufferObject
            ObjectLockOCCLongLock<K,V> objectDb = (ObjectLockOCCLongLock) buffer.getObjectDb();

            long local = buffer.getVersion();
            long global = objectDb.getVersion();

            if (LockHelper.getVersion(local) != LockHelper.getVersion(global) ||
                    (LockHelper.isLocked(global) && !lockObjects.contains(objectDb) )) {
                abortVersions(lockObjects);
                return false;
            }
        }

//        commitId = Database.timestamp.getAndIncrement();

        // Escrita
        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectLockOCCLongLock<K,V> objectDb = (ObjectLockOCCLongLock) buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
//            objectDb.unlock_write();
            version = buffer.getVersion()+1; // Increment version
            objectDb.compareAndSet(buffer.getVersion(), LockHelper.unlock(version)); //Unlock write
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
        long version;
        while (it_locks.hasNext()) {
            ObjectLockOCCLongLock<K,V> objectDb = (ObjectLockOCCLongLock) it_locks.next();
//            objectDb.unlock_write();
            version = objectDb.getVersion();
            objectDb.compareAndSet(version, LockHelper.unlock(version));
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
