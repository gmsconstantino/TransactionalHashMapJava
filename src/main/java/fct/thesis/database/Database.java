package fct.thesis.database;

import java.util.Iterator;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    protected Storage<K,V> storage;
    private ThreadCleaner cleaner;

    public Database(){
    }

    protected ObjectDb<K,V> getKey(int table, K key){
        return storage.getKey(table, key);
    }

    protected ObjectDb<K,V> putIfAbsent(int table, K key, ObjectDb<K,V> obj){
        return storage.putIfAbsent(table, key, obj);
    }

    protected ObjectDb<K,V> removeKey(int table, K key){
        return storage.removeKey(table, key);
    }

    public void cleanup(){
        cleaner.stop = true;
    }

    public void startThreadCleaner(ThreadCleaner _cleaner){
        cleaner = _cleaner;
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /*
     * Public
     */

    public void setStorage(Storage storage){
        this.storage = storage;
    }

    public Transaction<K,V> newTransaction(TransactionFactory.type t){
        return TransactionFactory.createTransaction(t, this);
    }

    public int size(int table){
        return storage.getSizeTable(table);
    }

//    public Iterator getIterator(int table) {
//        return new DbIterator(table);
//    }

    protected Iterator<ObjectDb<K,V>> getObjectDbIterator(int table){
        return storage.getObjectDbIterator(table);
    }



}
