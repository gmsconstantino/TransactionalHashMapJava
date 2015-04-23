package fct.thesis.databaseOCC;

import fct.thesis.database.Database;
import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestDatabaseOCC {

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

        Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

        t.put(10, 5);
        t.put( 20, 15);

        t.commit();

        t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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
        assertEquals(null, v2[0]);
        assertEquals(null, v2[1]);
    }

    @org.junit.Test
    public void testSequentialPutGetCommit() throws Exception {

        final Integer[] v2 = new Integer[2];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

        final Integer[] v2 = new Integer[3];

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

                t.put(10, 5);
                t.put(20, 15);
                t.put(25, 7);

                t.abort();

            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

        for (Integer v : v2){
            assertEquals(null, v);
        }
    }

//    @org.junit.Test
    public void testThreadPut() throws Exception {

        final Integer[] v2 = new Integer[6];
        final Long[] transaction_commit = new Long[2];

        long commit_id = Database.timestamp.get();

        Thread t1 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

                for (int i = 1; i <= 5; i++) {
                    t.put(i*10, i);
                }

                if(!t.commit()){
                    System.out.println("[abort] "+t);
                }
                transaction_commit[0] = t.getCommitId();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

                for (int i = 1; i <= 5; i++) {
                    t.put(i*10, i+20);
                }

                if(!t.commit()){
                    System.out.println("[abort] "+t);
                }
                transaction_commit[1] = t.getCommitId();
            }
        };

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        Thread t3 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer, Integer> t = db.newTransaction(TransactionFactory.type.OCC);
                for (int i = 1; i <= 5; i++) {
                    v2[i] = t.get(i * 10);
                }
                t.commit();
            }
        };

        t3.start();
        t3.join();


        if (transaction_commit[0] != null) { /// T1 < T2
            for (int j = 1; j <= 5; j++) {
                    assertEquals(j, v2[j].intValue());
            }
        } else {
            for (int j = 1; j <= 5; j++) {
                assertEquals(j+20, v2[j].intValue());
            }
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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

//                v2[0] = t.get_to_update(10);
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


        assertEquals(105, v2[0].intValue());
        assertEquals(15, v2[1].intValue());

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

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

                t.put(10, 5);
                t.put(20, 15);

                t.commit();
            }
        };

        Thread t2 = new Thread(){
            @Override
            public void run() {
                super.run();

                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

//                v2[0] = t.get_to_update(10);
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
                    Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);
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

        assertEquals(105, v2[0].intValue());
        assertEquals(15, v2[1].intValue());

    }

}