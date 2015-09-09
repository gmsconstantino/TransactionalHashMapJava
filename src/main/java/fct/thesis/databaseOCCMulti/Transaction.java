package fct.thesis.databaseOCCMulti;

import fct.thesis.database.*;
import fct.thesis.structures.P;
import sun.misc.Contended;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends TransactionAbst<K,V> {

    @Contended
    static AtomicInteger timestamp = new AtomicInteger(0);

    protected Map<P<Integer,K>, Long> readSet; //set , conf se add nao altera
    protected Map<P<Integer,K>, BufferDb<K,V>> writeSet;

    private long startTime;
    private long commitTime;

    public Transaction(Database db) {
        super(db);
    }

    protected synchronized void init(){
        super.init();
        readSet = new HashMap<>();
        writeSet = new TreeMap<>();

        startTime = timestamp.get();
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

        if (readSet.containsKey(key)){
                returnValue = obj.getValueVersionLess(readSet.get(key));
        } else {
            addObjectDbToReadBuffer(key, obj.getVersion());
            returnValue = (V) obj.getValueVersionLess(startTime);
        }
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

        ObjectMultiVersionLockDB<K,V> obj = (ObjectMultiVersionLockDB) getKeyDatabase(table, k);
        //Nao existe nenhuma
        if (obj == null) {
            obj = new ObjectMultiVersionLockDB<K,V>(); // A thread fica com o write lock
            ObjectMultiVersionLockDB<K,V> objdb = (ObjectMultiVersionLockDB) putIfAbsent(table, k, obj);

            if (objdb != null)
                obj = objdb;
        }

        BufferDb<K,V> buffer = new BufferObjectDb(table, k, value, obj.getVersion(), obj);
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

        Set<ObjectLockDb<V>> lockObjects = new HashSet<ObjectLockDb<V>>();

        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) buffer.getObjectDb();

            objectDb.lock_write();
            lockObjects.add(objectDb);

            // buffer.version != objectDb.last_version
            if (buffer.getVersion() != objectDb.getVersion()) {
                abortVersions(lockObjects);
                return false;
            }
        }


        // Validate Read Set
        for (Map.Entry<P<Integer,K>, Long> obj_readSet : readSet.entrySet()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) getKeyDatabase(obj_readSet.getKey().f, obj_readSet.getKey().s);

            // readSet.version != objectDb.last_version
            if (obj_readSet.getValue() != objectDb.getVersion()) {
                abortVersions(lockObjects);
                return false;
            }
        }

        commitId = Transaction.timestamp.getAndIncrement();
        // Escrita
        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) buffer.getObjectDb();
            objectDb.addNewVersionObject(commitId, buffer.getValue());
            objectDb.unlock_write();
        }

        isActive = false;
        success = true;
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
    }


    void addObjectDbToReadBuffer(P key, Long version){
        readSet.put(key, version);
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
