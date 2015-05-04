package fct.thesis.databaseOCCRDIAS;

import fct.thesis.database.*;

import java.util.*;


/**
 * Created by gomes on 26/02/15.
 */

public class Transaction<K extends Comparable<K>,V> extends fct.thesis.database.Transaction<K,V> {

    protected List<RWEntry<K, V>> readSet;
    protected Map<K, RWEntry<K, V>> writeSet;

    public Transaction(Database db) {
        super(db);

    }

    protected void init(){
        super.init();

        readSet = new ArrayList<>(10);
        writeSet = new TreeMap<>();
    }

    @Override
    public V get(K key) throws TransactionTimeoutException, TransactionAbortException {
        if (!isActive)
            return null;

        if(writeSet.containsKey(key)){
            return (V) writeSet.get(key).newValue;
        }


        MyObject<K,V> obj = (MyObject) getKeyDatabase(key);

        long ver = obj.getVersion();
        V returnValue = obj.getValue();

        readSet.add(new RWEntry<>(obj, ver, null, null, false));


        return returnValue;
    }

    @Override
    public void put(K key, V value) throws TransactionTimeoutException{
        if(!isActive)
            return;

        if(writeSet.containsKey(key)){
            writeSet.get(key).newValue = value; // set new value on buffer
            return;
        }

        MyObject<K,V> obj = (MyObject) getKeyDatabase(key);
        writeSet.put(key, new RWEntry<>(obj, -1, key, value, obj == null));


    }

    private static <K, V> void unlockAll(List<MyObject<K,V>> locks) {
        for (MyObject<K,V> lock : locks) {
            lock.unlock();
        }
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        if(!isActive)
            return success;

        int tid = (int) Thread.currentThread().getId();

        List<MyObject<K,V>> locks = new ArrayList<>();
        for (RWEntry<K, V> write : writeSet.values()) {
            if (write.isNew) {
                MyObject<K, V> newObj = new MyObject<>();
                write.obj = newObj;
                newObj.lock(tid);
                ObjectDb<K, V> oldObj = putIfAbsent(write.key, newObj);
                if (oldObj != null) {
                    write.obj = (MyObject<?, V>) oldObj;
                    write.obj.lock(tid);
                }
                locks.add((MyObject<K, V>) write.obj);
            }
            else {
                write.obj.lock(tid);
                locks.add((MyObject<K, V>) write.obj);
            }
        }

        for (RWEntry<K, V> read : readSet) {
            if (!read.obj.checkVersion(tid, read.version)) {
                unlockAll(locks);
                abort();
                throw new TransactionAbortException();
            }
        }


         for (RWEntry<K, V> write : writeSet.values()) {
             long newVersion = write.obj.getVersion()+1;
             write.obj.setValue(write.newValue);
             write.obj.unlock(newVersion);
         }



        isActive = false;
        success = true;
        return true;
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
