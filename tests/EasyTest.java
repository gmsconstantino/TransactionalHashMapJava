import fct.thesis.database.*;
import fct.thesis.database.Transaction;
import fct.thesis.databaseNMSI.*;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by gomes on 10/03/15.
 */
public class EasyTest {

    public static class Tx implements Comparable {
        long v;

        public Tx(long v) {
            this.v = v;
        }

        @Override
        public int compareTo(Object o) {
            Tx t = (Tx) o;
            return Long.compare(v, t.v);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Tx)) return false;

            Tx tx = (Tx) o;

            if (v != tx.v) return false;

            return true;
        }

        @Override
        public String toString() {
            return "Tx{" +
                    "v=" + v +
                    '}';
        }
    }

    public static Database<Integer,Integer> db;

    public static class Worker extends Thread {

        public void run() {
            for (int i = 0; i < 10; i++){
                Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.NMSI);
                t.commit();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {

//        Database<Integer, Integer> db = new Database<Integer, Integer>();
//
//        Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);
//
//        t.put(10,5);
//
//        t.abort();
//
//        t = db.newTransaction(TransactionFactory.type.OCC);
//
//        Integer n = t.get(10);
//
//        t.commit();

        TreeMap<Tx, Integer> map = new TreeMap<>();
        map.put(new Tx(3),5);
        Tx t = new Tx(6);
//        map.put(t,3);
        map.put(new Tx(9),8);

        System.out.println(map.get(t));
        System.out.println(map.ceilingKey(t));
        System.out.println();

        // -------------------------------------

        Random r = new Random();

        PriorityBlockingQueue<Tx> queueTx = new PriorityBlockingQueue(1000000);

        for (int i = 0; i < 20; i++) {
            queueTx.add(new Tx(r.nextInt(100)));
        }

        for (Tx i = queueTx.poll(); !queueTx.isEmpty(); i = queueTx.poll()){
            System.out.println(i);
        }
        System.out.println();

        // -------------------------------------

        db = new Database<Integer, Integer>(5);

        Worker[] workers = new Worker[5];

        for (int i=0; i < workers.length; i++) {
            workers[i] = new Worker();
            workers[i].start();
        }

        for (int i=0; i < workers.length; i++) {
            workers[i].join();
        }

        System.out.println(Arrays.toString(fct.thesis.databaseNMSI.Transaction.shardIds));

        PriorityBlockingQueue<Transaction> queue = Database.queue;
        while (!queue.isEmpty()){
            System.out.println(queue.poll());
        }

    }

}
