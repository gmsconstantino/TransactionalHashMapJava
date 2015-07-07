package fct.thesis.databaseSI;

import fct.thesis.database.*;
import fct.thesis.databaseOCCMulti.ObjectMultiVersionLockDB;
import sun.misc.Contended;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends fct.thesis.database.Transaction<K,V> {

    @Contended
    public static AtomicInteger timestamp = new AtomicInteger(0);

    protected Map<K, BufferObjectDb<K,V>> writeSet;


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
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        long st = System.nanoTime();

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).getValue();
        }

        V returnValue = null;

        ObjectMultiVersionLockDB<K,V> obj = (ObjectMultiVersionLockDB) getKeyDatabase(key);
        if (obj == null || obj.getVersion() == -1)
            return null;

        returnValue = obj.getValueVersionLess(id);

        long en = System.nanoTime();
        int index = (int) Thread.currentThread().getId()%100;
        tget[index] += (en-st)/1000;

        return returnValue;
    }

    @Override
    public void put(K key, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        if(writeSet.containsKey(key)){
            writeSet.get(key).setValue(value);
            return;
        }

        // o objecto esta na base de dados
        BufferObjectDb<K,V> buffer = new BufferObjectDb(key, value);
        addObjectDbToWriteBuffer(key, buffer);
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        if (writeSet.size() == 0){
            st = System.nanoTime();

            isActive = false;
            success = true;
            ThreadCleanerSI.set.remove(this);

            long en = System.nanoTime();
            int index = (int) Thread.currentThread().getId()%100;
            ncommit[index]++;
            tcommit[index] += (en-st)/1000;
            return true;
        }

        st = System.nanoTime();
        Set<ObjectLockDb<K,V>> lockObjects = new HashSet<ObjectLockDb<K,V>>();

        for (BufferObjectDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) getKeyDatabase(buffer.getKey());
            //Nao existe nenhuma
            if (objectDb == null) {
                objectDb = new ObjectMultiVersionLockDB<K,V>(); // A thread fica com o write lock
                ObjectMultiVersionLockDB<K,V> objdb = (ObjectMultiVersionLockDB) putIfAbsent(buffer.getKey(), objectDb);

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
                long f = System.nanoTime();
                abortVersions(lockObjects);

                long en = System.nanoTime();
                int index = (int) Thread.currentThread().getId()%100;
                nabort[index]++;
                tabort[index] += (en-st)/1000;

                debug1[index] += (f-st)/1000;
                debug2[index] += (en-f)/1000;

                return false;
            }
        }

        long xst = System.nanoTime();

        commitId = timestamp.getAndIncrement();
        // Escrita
        for (BufferDb<K,V> buffer : writeSet.values()){
            ObjectMultiVersionLockDB<K,V> objectDb = (ObjectMultiVersionLockDB) buffer.getObjectDb();
            objectDb.addNewVersionObject(commitId, buffer.getValue());
            objectDb.unlock_write();
        }

        long xen = System.nanoTime();

        isActive = false;
        success = true;
        ThreadCleanerSI.set.remove(this);

        long en = System.nanoTime();
        int index = (int) Thread.currentThread().getId()%100;
        ncommit[index]++;
        tcommit[index] += (en-st)/1000;
        tXcommit[index] += (xen-xst)/1000;

//        debug[index] += (en-stdebug)/1000;

        return true;
    }

    private void abortTimeout(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();
        throw new TransactionTimeoutException("Transaction " + getId() +": Thread "+Thread.currentThread().getName()+" - commit");
    }

    private void abortVersions(Set<ObjectLockDb<K,V>> lockObjects) throws TransactionTimeoutException{
        unlockWrite_objects(lockObjects);
        abort();

//        throw new TransactionAbortException("Transaction Abort " + getId() +": Thread "+Thread.currentThread().getName()+" - Version change");
    }

    private void unlockWrite_objects(Set<ObjectLockDb<K,V>> set){
        Iterator<ObjectLockDb<K,V>> it_locks = set.iterator();
        while (it_locks.hasNext()) {
            ObjectLockDb<K,V> objectDb = it_locks.next();
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

    void addObjectDbToWriteBuffer(K key, BufferObjectDb objectDb){
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

    @Override
    public Collection getWriteSet() {
        return writeSet.values();
    }

}
