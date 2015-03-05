import database.Database;
import database.Transaction;
import database.TransactionFactory;

import javax.xml.crypto.Data;
import java.util.Iterator;

import static org.junit.Assert.*;

public class MainShopTest {

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
        final Integer[] v2 = new Integer[2];

        Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

        t.put(10, 5);
        t.put( 20, 15);

        t.commit();

        t = db.newTransaction(TransactionFactory.type.TWOPL);

        v2[0] = t.get(10);
        v2[1] = t.get(20);

        t.commit();

        assertEquals(v2[0].intValue(), 5);
        assertEquals(v2[1].intValue(), 15);
    }

    @org.junit.Test
    public void testSequentialPutGetAbort() throws Exception {

        final Integer[] v2 = new Integer[2];


        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);

                t.get(10);

                try {
                    t.abort();

                } catch (NullPointerException e){
                    fail();
                }

            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get(10);

                v2[1] = t.get(20);

                t.commit();

            }
        };

        t1.start();
        t1.join();

        t2.start();
        t2.join();

//        if ()
        assertEquals(v2[0], null);
        assertEquals(v2[1], null);
    }

    @org.junit.Test
    public void testSequentialPutGetCommit() throws Exception {

        final Integer[] v2 = new Integer[2];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);

                t.get(10);

                t.commit();

            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get(10);

                v2[1] = t.get(20);

                t.commit();

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
    public void testSequentialPut() throws Exception {

        final Integer[] v2 = new Integer[2];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);

//                Thread.yield();

                t.put(20, 15);

                t.commit();

            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get(10);

                v2[1] = t.get(20);

                t.commit();

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
    public void testSequentialPut_2() throws Exception {

        final Integer[] v2 = new Integer[3];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);
                t.put(25, 7);

                t.put(10, 9);
                t.put(20, 1);

                t.commit();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get(10);
                v2[1] = t.get(20);
                v2[2] = t.get(25);

                t.commit();

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
    public void testAbort() throws Exception {

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);
                t.put(25, 7);

                t.abort();

            }
        };



        t1.start();
        t1.join();

        assertEquals(db.size(), 0);
    }

    @org.junit.Test
    public void testThreadPut() throws Exception {

        final Integer[] v2 = new Integer[2];
        final Long[] transaction_commit = new Long[2];

        long commit_id = Database.timestamp.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);

                t.commit();
                transaction_commit[0] = t.getCommitId();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get(10);
                v2[1] = t.get(20);

                t.commit();
                transaction_commit[1] = t.getCommitId();
            }
        };

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        if (transaction_commit[0] < transaction_commit[1]) {
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

        long commit_id = Database.timestamp.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);

                t.commit();
                transaction1_id[0] = t.getCommitId();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get_to_update(10);
                t.put(10, 105);

                v2[0] = t.get(10);
                v2[1] = t.get(20);

                t.commit();
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

        long commit_id = Database.timestamp.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                t.put(10, 5);
                t.put(20, 15);

                t.commit();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);

                v2[0] = t.get_to_update(10);
                assertEquals(v2[0].intValue(), 5);

                t.put(10, 105);

                v2[0] = t.get(10);
                v2[1] = t.get(20);

                t.commit();
            }
        };

        Thread t3 = new Thread(){
            @Override
            public void run() {
                super.run();
                long n;
                while (stop_t3[0] == 1L) {
                    Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.TWOPL);
                    t.get(10);
                    t.commit();

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