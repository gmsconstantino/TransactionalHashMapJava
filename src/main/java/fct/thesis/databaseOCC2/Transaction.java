package fct.thesis.databaseOCC2;

import fct.thesis.database.*;
import fct.thesis.database2PL.Config;
import pt.dct.util.P;

import java.util.*;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K,V> extends fct.thesis.database.Transaction<K,V> {

    protected Map<K,Long> readSet;
    protected Map<K, ObjectOCC2<K,V>> writeSet;

    public Transaction(Database db) {
        super(db);
    }

    protected void init(){
        super.init();

        readSet = new HashMap<K,Long>();
        writeSet = new HashMap<K, ObjectOCC2<K, V>>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if (writeSet.containsKey(key)) {
            return (V) writeSet.get(key).getValue();
        }

        V returnValue;

        ObjectOCC2<K, V> obj = (ObjectOCC2) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        if (readSet.containsKey(key)){
            try {
                returnValue = obj.readVersion(readSet.get(key));
            } catch (TransactionAbortException e){
                abort();
                throw new TransactionAbortException("GET: Transaction Abort " + getId() +": "+Thread.currentThread().getName()+" - Version change - key:"+key);
            }
        } else {
            P<V,Long> r = obj.readLast();
            readSet.put(key, r.s);
            returnValue = r.f;
        }

        return returnValue;
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
            ObjectDb<K,V> objectDb = writeSet.get(key);
            objectDb.setValue(value);
            return;
        }

        ObjectOCC2<K,V> obj = (ObjectOCC2) getKeyDatabase(key);
        if (obj == null) {
            obj = new ObjectOCC2Impl<K,V>(null); // A thread fica com o write lock
            ObjectOCC2<K,V> objdb = (ObjectOCC2) putIfAbsent(key, obj);

            if (objdb != null)
                obj = objdb;
        }

        ObjectOCC2<K,V> buffer = new BufferOCC2(key, value, obj.getVersion(), obj);
        writeSet.put(key, buffer);
        return;
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        Set<ObjectOCC2<K,V>> lockObjects = new HashSet<ObjectOCC2<K,V>>();

        for (ObjectOCC2<K,V> buffer : writeSet.values()){
            ObjectOCC2<K,V> objectDb = (ObjectOCC2) buffer.getObjectDb();
            if(objectDb.try_lock(Config.TIMEOUT, Config.TIMEOUT_UNIT)){
                lockObjects.add(objectDb);

                if (buffer.getVersion() == objectDb.getVersion()) {
                    continue;
                } else {
                    abortVersions(lockObjects);
                    return false;
                }
            } else {
                abortTimeout(lockObjects);
                return false;
            }
        }

        // Validate Read Set
        for (Map.Entry<K,Long> buffer : readSet.entrySet()){ // BufferObject
            ObjectOCC2<K,V> objectDb = (ObjectOCC2) getKeyDatabase(buffer.getKey());
                if (buffer.getValue() == objectDb.getVersion()) {
                    continue;
                } else {
                    abortVersions(lockObjects);
                    return false;
                }
        }

        // Escrita
        for (ObjectOCC2<K,V> buffer : writeSet.values()){
            ObjectOCC2<K,V> objectDb = (ObjectOCC2) buffer.getObjectDb();
            objectDb.setValue(buffer.getValue());
        }

        unlockWrite_objects(lockObjects);

        isActive = false;
        success = true;
        return true;
    }

    private void abortTimeout(Set<ObjectOCC2<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("COMMIT: Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectOCC2<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionAbortException("COMMIT: Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectOCC2<K,V>> set){
        Iterator<ObjectOCC2<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectOCC2<K,V> objectDb = it_locks.next();
            objectDb.unlock();
        }
    }

    @Override
    public void abort() throws TransactionAbortException{
        isActive = false;
        success = false;
        commitId = -1;
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
