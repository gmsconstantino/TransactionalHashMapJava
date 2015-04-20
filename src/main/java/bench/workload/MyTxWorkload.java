/**
 * Copyright (c) 2013 NOVA-LINCS
 * @Author Joao Leitao & Henrique Moniz
 */

package bench.workload;

import fct.thesis.database.*;
import bench.measurements.Measurements;

import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyTxWorkload {

    private volatile AtomicBoolean stopRequested = new AtomicBoolean(false);
    private Random random = new Random(System.currentTimeMillis());

    private int _keyspace;

    public MyTxWorkload(int keyspace) {
        this._keyspace = keyspace;
    }

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
     * synchronized, since each thread has its own threadstate instance.
     */
    public boolean doInsert(Database db, Object threadstate) {
        return true;
    }

    public boolean doTransaction(Database db, Object threadstate) {
        int txSize = 4;
        double txread = 0.5;
        int numberOfReads = (int) Math.round(txSize * txread);
        int numberOfWrites = txSize - numberOfReads;
        Vector<Integer> keysRead = new Vector<Integer>();
        Vector<Integer> keysWrite = new Vector<Integer>();
        Integer k;
        for(int i = 0; i < numberOfReads; i++) {
            k = txNextKeynum();
            while(keysRead.contains(k)) k = txNextKeynum();
            keysRead.add(k);
        }
        int i = 0;
        for(; i< numberOfWrites; i++) {
            k = txNextKeynum();
            while(keysWrite.contains(k) && keysRead.contains(k)) k = txNextKeynum();
            keysWrite.add(k);
        }

        //do the transaction here.
        long st = System.nanoTime();

        TransactionFactory.type TYPE = TransactionFactory.type.OCC;
        Transaction<Integer,Integer> t = db.newTransaction(TYPE);
        try {
            for(Integer key: keysRead) {
                t.get(key);
            }

            for(Integer key: keysWrite) {
                t.put(key, random.nextInt(1000));
            }

            if(t.commit()) {
                long en = System.nanoTime();
                Measurements.getMeasurements().measure("Tx", (int)((en-st)/1000));
                Measurements.getMeasurements().reportReturnCode("Tx", 0);
            } else {
                Measurements.getMeasurements().reportReturnCode("Tx", -1);
            }
        } catch(TransactionTimeoutException e){
//            e.printStackTrace();
            Measurements.getMeasurements().reportReturnCode("Tx", -2);
        } catch (TransactionAbortException e){
//            System.err.println(e.getMessage());
            Measurements.getMeasurements().reportReturnCode("Tx", -3);
        }

        return true;
    }

    private Integer txNextKeynum() {
        return random.nextInt(_keyspace);
    }

    /**
     * Allows scheduling a request to reset the bench.workload.
     */
    public void resetStop() {
        stopRequested.set(false);
    }

    /**
     * Allows scheduling a request to stop the bench.workload.
     */
    public void requestStop() {
        stopRequested.set(true);
    }

    /**
     * Check the status of the stop request flag.
     * @return true if stop was requested, false otherwise.
     */
    public boolean isStopRequested() {
        if (stopRequested.get() == true) return true;
        else return false;
    }
}
