package database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Created by gomes on 26/02/15.
 */
public class Transaction implements Comparable {

    static long count = 0;

    long id;
    Set<Lock> lockList;

    public Transaction(){
        init();
    }

    private synchronized void init(){
        id = count++;
        lockList = new HashSet<Lock>();
    }

    void addLock(Lock l){
        lockList.add(l);
    }

    public void commit(){

    }

    public void abort(){

        unlock_locks();

    }

    private void unlock_locks(){
        for(Lock l : lockList){
            l.unlock();
        }
    }

    @Override
    public int compareTo(Object o) {
        Transaction t = (Transaction) o;
        return Long.compare(this.id, t.id);
    }
}
