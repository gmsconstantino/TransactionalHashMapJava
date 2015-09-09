package fct.thesis.databaseSI;

import fct.thesis.database.*;
import fct.thesis.databaseOCCMulti.ObjectMultiVersionLockDB;
import fct.thesis.structures.P;
import sun.misc.Contended;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends TransactionAbst<K,V> {

    @Contended
    public static AtomicInteger timestamp = new AtomicInteger(0);

    protected Map<P<Integer,K>, BufferObjectDb<K,V>> writeSet;


    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();
        writeSet = new TreeMap<>();
        ThreadCleanerSI.set.add(this);
        id = timestamp.get();
    }

    @Override
    public V get(int table, K k) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        P key = new P<>(table,k);
        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue = null;

        ObjectMultiVersionLockDB<K,V> obj = (ObjectMultiVersionLockDB) getKeyDatabase(table, k);
        if (obj == null || obj.getVersion() == -1)
            return null;

        returnValue = obj.getValueVersionLess(id);

        return returnValue;
    }

    @Override
    public void put(int table, K k, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        P key = new P<>(table,k);
        if(writeSet.containsKey(key)){
            writeSet.get(key).setValue(value);
            return;
        }

        // o objecto esta na base de dados
        BufferObjectDb<K,V> buffer = new BufferObjectDb(table, k, value);
        addObjectDbToWriteBuffer(key, buffer);
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        if (writeSet.size() == 0){
            isActive = false;
            success = true;
            ThreadCleanerSI.set.remove(this);
            return true;
        }

        Set<ObjectLockDb<V>> lockObjects = new HashSet<>();

        for (BufferObjectDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) getKeyDatabase(buffer.getTable(), buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                objectDb = new ObjectMultiVersionLockDB<K,V>(); // A thread fica com o write lock
                ObjectMultiVersionLockDB<K,V> objdb = (ObjectMultiVersionLockDB) putIfAbsent(buffer.getTable(), buffer.getKey(), objectDb);

                if (objdb != null)
                    objectDb = objdb;
            }

            objectDb.lock_write();
            lockObjects.add(objectDb);
            buffer.setObjectDb(objectDb);

            // buffer.version == objectDb.last_version
            if (objectDb.getVersion() < id) {
                continue;
            } else {
                abortVersions(lockObjects);
                return false;
            }
        }


        commitId = timestamp.getAndIncrement();
        // Escrita
        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) buffer.getObjectDb();
            objectDb.addNewVersionObject(commitId, buffer.getValue());
            objectDb.unlock_write();
        }

        isActive = false;
        success = true;
        ThreadCleanerSI.set.remove(this);
        return true;
    }

    private void abortTimeout(Set<ObjectLockDb<V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectLockDb<V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
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
        ThreadCleanerSI.set.remove(this);
    }

    void addObjectDbToWriteBuffer(P key, BufferObjectDb objectDb){
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
