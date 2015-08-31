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

    private ThreadCleaner cleaner;

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
        cleaner.stop = true;
        try {
            long ncommit = 0;
            for (int i = 0; i < Transaction.ncommit.length; i++) {
                ncommit += Transaction.ncommit[i];
            }

            long nabort = 0;
            for (int i = 0; i < Transaction.nabort.length; i++) {
                nabort += Transaction.nabort[i];
            }

            long nget = 0;
            for (int i = 0; i < Transaction.nget.length; i++) {
                nget += Transaction.nget[i];
            }

            float tget = 0;
            for (int i = 0; i < Transaction.tget.length; i++) {
                tget += Transaction.tget[i];
            }
            tget /= nget;

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

            float tdebug1 = 0;
            for (int i = 0; i < Transaction.debug1.length; i++) {
                tdebug1 += Transaction.debug1[i];
            }
            tdebug1 /= nabort;

            float tdebug2 = 0;
            for (int i = 0; i < Transaction.debug2.length; i++) {
                tdebug2 += Transaction.debug2[i];
            }
            tdebug2 /= nabort;

            File f = new File("/local/cj.gomes/result/commit.csv");
            boolean x = !f.exists();
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f.getAbsolutePath(), true)));
            if (x)
                pw.println("ncommit,t_commit,time_mutex,nabort,tabort,t_debug1,t_debug2,tget");
            pw.append(ncommit+","+tcommit+","+tXcommit+","+nabort+","+tabort+","+tdebug1+","+tdebug2+","+tget+"\n");

            pw.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        
    }

    public void startThreadCleaner(ThreadCleaner _cleaner){
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
