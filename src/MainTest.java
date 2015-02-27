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
    public void test2SequenatialPut() throws Exception {

        final Integer[] v2 = new Integer[2];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.txn_begin();

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

                Transaction<Integer,Integer> t = db.txn_begin();

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
    public void test2ThreadPut() throws Exception {

        final Integer[] v2 = new Integer[2];
        final Long[] transaction1_id = new Long[1];

        long commit_id = Database.commit_count;

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.txn_begin();

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

                Transaction<Integer,Integer> t = db.txn_begin();

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
    public void test2ThreadGetAndUpdate() throws Exception {

        final Integer[] v2 = new Integer[2];
        final Long[] transaction1_id = new Long[1];

        long commit_id = Database.commit_count;

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.txn_begin();

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

                Transaction<Integer,Integer> t = db.txn_begin();

                v2[0] = db.get(t,10);
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

}