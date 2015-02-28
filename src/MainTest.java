import database.Database;
import database.Transaction;
import org.junit.Test;

import static org.junit.Assert.*;

public class MainTest {

    Database<Integer,Integer> db;

    @org.junit.Before
    public void setUp() throws Exception {
        db = new Database<Integer, Integer>();
    }

    @org.junit.After
    public void tearDown() throws Exception {

    }

    @org.junit.Test
    public void testPopulate_database() throws Exception {
        assertTrue(true);
    }

    @org.junit.Test
    public void testSequenatialPut() throws Exception {

        final Integer[] v2 = new Integer[2];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                db.put(t,10, 5);

//                Thread.yield();

                db.put(t, 20, 15);

                db.txn_commit(t);

            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                v2[0] = db.get(t,10);

                v2[1] = db.get(t,20);

                db.txn_commit(t);

            }
        };

        t1.start();
        t1.join();

        t2.start();
        t2.join();

//        if ()
        assertEquals(v2[0].intValue(), 5);
        assertEquals(v2[1].intValue(), 15);
    }

    @org.junit.Test
    public void testSequenatialPut_2() throws Exception {

        final Integer[] v2 = new Integer[3];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                db.put(t,10, 5);
                db.put(t,20, 15);
                db.put(t,25, 7);

                db.put(t,10, 9);
                db.put(t,20, 1);

                db.txn_commit(t);

            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                v2[0] = db.get(t,10);
                v2[1] = db.get(t,20);
                v2[2] = db.get(t,25);

                db.txn_commit(t);

            }
        };

        t1.start();
        t1.join();

        t2.start();
        t2.join();

        assertEquals(v2[0].intValue(), 9);
        assertEquals(v2[1].intValue(), 1);
        assertEquals(v2[2].intValue(), 7);
    }

    @org.junit.Test
    public void testThreadPut() throws Exception {

        final Integer[] v2 = new Integer[2];
        final Long[] transaction1_id = new Long[1];

        long commit_id = Transaction.commit_count.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                db.put(t,10, 5);
                db.put(t, 20, 15);

                db.txn_commit(t);
                transaction1_id[0] = t.getCommit_id();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                v2[0] = db.get(t,10);
                v2[1] = db.get(t,20);

                db.txn_commit(t);
            }
        };

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        if (transaction1_id[0] == commit_id) {
            assertEquals(v2[0].intValue(), 5);
            assertEquals(v2[1].intValue(), 15);
        } else {
            assertEquals(v2[0],null);
            assertEquals(v2[1],null);
        }
    }

    @org.junit.Test
    public void testThreadGetAndUpdate() throws Exception {

        final Integer[] v2 = new Integer[2];
        final Long[] transaction1_id = new Long[1];

        long commit_id = Transaction.commit_count.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                db.put(t,10, 5);
                db.put(t, 20, 15);

                db.txn_commit(t);
                transaction1_id[0] = t.getCommit_id();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                v2[0] = db.get_to_update(t,10);
                db.put(t,10, 105);

                v2[0] = db.get(t,10);
                v2[1] = db.get(t,20);

                db.txn_commit(t);
            }
        };

        t1.start();
        t1.join();

        t2.start();
        t2.join();


        assertEquals(v2[0].intValue(), 105);
        assertEquals(v2[1].intValue(), 15);

    }

    @org.junit.Test
    public void testThreadGetAndUpdate_2() throws Exception {

        final Integer[] v2 = new Integer[2];
        final Long[] stop_t3 = new Long[1];

        long commit_id = Transaction.commit_count.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                db.put(t,10, 5);
                db.put(t, 20, 15);

                db.txn_commit(t);
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer> t = db.txn_begin();

                v2[0] = db.get_to_update(t,10);
                assertEquals(v2[0].intValue(), 5);

                db.put(t,10, 105);

                v2[0] = db.get(t,10);
                v2[1] = db.get(t,20);

                db.txn_commit(t);
            }
        };

        Thread t3 = new Thread(){
            @Override
            public void run() {
                super.run();
                long n;
                while (stop_t3[0] == 1L) {
                    Transaction<Integer> t = db.txn_begin();
                    db.get(t, 10);
                    db.txn_commit(t);

                    for (int i = 0; i < 1000000; i++) {
                        n = i;
                    }
                }
            }
        };

        stop_t3[0] = 1L;

        t1.start();
        t1.join();

        t3.start();
        t2.start();
        t2.join();

        stop_t3[0] = 0L;
        t3.join();

        assertEquals(v2[0].intValue(), 105);
        assertEquals(v2[1].intValue(), 15);

    }

}