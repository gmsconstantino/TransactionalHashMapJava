package fct.thesis.databaseOCCRDIAS;

import fct.thesis.database.ObjectDb;

import java.util.concurrent.atomic.AtomicLong;


public class MyObject<K,V> implements ObjectDb<K,V> {

    private static final long LOCK_MASK = 1L << 63;
    private static final long UNLOCK_MASK = ~LOCK_MASK;
    private static final long TID_MASK = 0xFFL << 55;
    private static final long VERSION_MASK = ~TID_MASK & UNLOCK_MASK;

    private AtomicLong version = new AtomicLong(0L);
    private V value;

    @Override
    public ObjectDb getObjectDb() {
        return this;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    public long getVersion() {
        return version.get() & VERSION_MASK;
    }

    public void lock(int tid) {

        long local = version.get();

        // Object is locked and I'm the owner
        if ((local & LOCK_MASK) != 0 && (local & TID_MASK) >>> 55 == tid)
            return;

        do {
            local = version.get();

            // Object is locked
            if ((local & LOCK_MASK) != 0)
                continue;

            long newLocal = local | LOCK_MASK;
            newLocal |= ((long) tid << 55);

            // Object was concurrently locked
            if (version.compareAndSet(local, newLocal))
                return;

        } while(true);
    }

    public boolean checkVersion(int tid, long clock) {
        long local = version.get();

        // Object is locked and I'm not the owner
        if ((local & LOCK_MASK) != 0 && (local & TID_MASK) >>> 55 != tid) {
            /*System.out.println("----------------------------");
            System.out.println("Locked: "+((version.get() & LOCK_MASK) != 0));
            System.out.println("TID: "+ ((version.get() & TID_MASK) >>> 55));
            System.out.println("Version: "+(version.get() & VERSION_MASK));
            System.out.println("Clock&Tid: "+clock+", "+tid);*/
            return false;
        }

        boolean result = (local & VERSION_MASK) <= clock;

        /*if (!result) {
            System.out.println("----------------------------");
            System.out.println("Locked: "+((version.get() & LOCK_MASK) != 0));
            System.out.println("TID: "+ ((version.get() & TID_MASK) >>> 55));
            System.out.println("Version: "+(version.get() & VERSION_MASK));
            System.out.println("Clock&Tid: "+clock+", "+tid);
        } */

        return result;
    }


    public void unlock() {
        version.set(version.get() & VERSION_MASK);
    }

    public void unlock(long clock) {
        version.set(clock & UNLOCK_MASK);
    }


    private long objectVersion(){
        return version.get();
    }

    public static void main(String[] args) {
        MyObject o = new MyObject();

        System.out.println(o.version.get());
        o.lock(3);
        System.out.println("Locked: "+((o.version.get() & LOCK_MASK) != 0));
        System.out.println("TID: "+ ((o.version.get() & TID_MASK) >>> 55));
        System.out.println(o.checkVersion(3, 0));
        o.unlock(o.getVersion()+1);
        System.out.println(o.getVersion());
        System.out.println("Locked: "+((o.version.get() & LOCK_MASK) != 0));
        System.out.println("TID: "+ ((o.version.get() & TID_MASK) >>> 55));
        System.out.println(o.checkVersion(4, 4));

        System.out.println();
        o.lock(171);
        System.out.println(Long.toBinaryString(o.objectVersion()));
        o.unlock();
        System.out.println("0"+Long.toBinaryString(o.objectVersion()));

        o.lock(212);
        System.out.println(Long.toBinaryString(o.objectVersion()));
        System.out.println(o.checkVersion(212, 0));
        o.unlock(o.getVersion()+1);

        System.out.println("0"+Long.toBinaryString(o.objectVersion()));

    }
}
