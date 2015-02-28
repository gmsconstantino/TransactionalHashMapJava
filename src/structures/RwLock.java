package structures;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by gomes on 26/02/15.
 */
public class RwLock {

    public final ReentrantReadWriteLock lock;
    private final Lock r;
    private final Lock w;

    public RwLock(){
        lock = new ReentrantReadWriteLock();
        r = lock.readLock();
        w = lock.writeLock();
    }

    public void lock_write(){
        w.lock();
    }

    public void unlock_write(){
        for (int i = 0; i < lock.getWriteHoldCount()+1; i++) {
            w.unlock();
        }
    }

    public boolean try_lock_write_for(long time, TimeUnit unit){
        try {
            return w.tryLock(time, unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void lock_read(){
        r.lock();
    }

    public void unlock_read(){
        for (int i = 0; i < lock.getReadHoldCount()+1; i++) {
            r.unlock();
        }
    }

    public boolean try_lock_read_for(long time, TimeUnit unit){
        try {
            return r.tryLock(time, unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean upgrade_lock(){
        unlock_read();
        return try_lock_write_for(1000, TimeUnit.MICROSECONDS);
    }

//    public Lock getObjectLock(){
//        if(lock.isWriteLockedByCurrentThread()){
//            return w;
//        } else
//            return r;
//    }

}
