package database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Created by gomes on 26/02/15.
 */
public class Transaction {

    Set<Lock> lockList;

    public Transaction(){
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

}
