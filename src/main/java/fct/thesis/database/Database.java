package fct.thesis.database;

import fct.thesis.structures.MapEntry;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> {

    public final static ExecutorService asyncPool = Executors.newFixedThreadPool(2);

    private Thread cleaner;

    public ConcurrentHashMap<K, ObjectDb<K,V>> concurrentHashMap;

    public Database(){
        concurrentHashMap = new ConcurrentHashMap<>(1000000,0.75f,64);
    }

    protected ObjectDb<K,V> getKey(K key){
        return concurrentHashMap.get(key);
    }

    protected ObjectDb<K,V> putIfAbsent(K key, ObjectDb<K,V> obj){
        return concurrentHashMap.putIfAbsent(key, obj);
    }

    protected ObjectDb<K,V> removeKey(K key){
        return concurrentHashMap.remove(key);
    }

    public void cleanup(){
        asyncPool.shutdownNow();

        try {
            long ncommit = 0;
            for (int i = 0; i < Transaction.ncommit.length; i++) {
                ncommit += Transaction.ncommit[i];
            }

            long nabort = 0;
            for (int i = 0; i < Transaction.nabort.length; i++) {
                nabort += Transaction.nabort[i];
            }

            float tabort = 0;
            for (int i = 0; i < Transaction.tabort.length; i++) {
                tabort += Transaction.tabort[i];
            }
            tabort /= nabort;

            float tcommit = 0;
            for (int i = 0; i < Transaction.tcommit.length; i++) {
                tcommit += Transaction.tcommit[i];
            }
            tcommit /= ncommit;

            float tXcommit = 0;
            for (int i = 0; i < Transaction.tXcommit.length; i++) {
                tXcommit += Transaction.tXcommit[i];
            }
            tXcommit /= ncommit;

            float tcleaner = 0;
            for (int i = 0; i < Transaction.debug.length; i++) {
                tcleaner += Transaction.debug[i];
            }
            tcleaner /= nabort;

            File f = new File("/local/cj.gomes/result/commit.csv");
            boolean x = !f.exists();
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f.getAbsolutePath(), true)));
            if (x)
                pw.println("ncommit,t_commit,time_mutex,nabort,tabort,t_cleaner");
            pw.append(ncommit+","+tcommit+","+tXcommit+","+nabort+","+tabort+","+tcleaner+"\n");

            pw.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void startThreadCleaner(Thread _cleaner){
        cleaner = _cleaner;
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /*
     * Public
     */

    public Transaction<K,V> newTransaction(TransactionFactory.type t){
        return TransactionFactory.createTransaction(t, this);
    }

    public int size(){
        return concurrentHashMap.size();
    }

    protected Iterator<ObjectDb<K,V>> getObjectDbIterator(){
        return new ObjectDbIterator();
    }

    public Iterator getIterator() {
        return new DbIterator();
    }

    private class DbIterator implements Iterator {

        Set<Map.Entry<K, ObjectDb<K,V>>> set;
        Iterator<Map.Entry<K, ObjectDb<K,V>>> it;
        DbIterator(){
            set = concurrentHashMap.entrySet();
            it = set.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {

            if(this.hasNext()){
                Map.Entry<K, ObjectDb<K,V>> obj = it.next();
                ObjectDb<K,V> objectDb = obj.getValue();
                return new MapEntry<K,V>(obj.getKey(),objectDb.getValue());
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

    private class ObjectDbIterator implements Iterator {

        Set<Map.Entry<K, ObjectDb<K,V>>> set;
        Iterator<Map.Entry<K, ObjectDb<K,V>>> it;
        ObjectDbIterator(){
            set = concurrentHashMap.entrySet();
            it = set.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Object next() {

            if(this.hasNext()){
                Map.Entry<K, ObjectDb<K,V>> obj = it.next();
                return obj.getValue();
            }
            return null;
        }

        @Override
        public void remove() {

        }
    }

}
