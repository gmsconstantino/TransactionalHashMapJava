package bench;

import fct.thesis.database.*;
import fct.thesis.databaseNMSI.ThreadCleanerNMSI;
import fct.thesis.databaseSI.ThreadCleanerSI;
import fct.thesis.storage.MultiHashMapStorage;

import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by gomes on 11/04/15.
 */
public class MicroTimeout {

    public static volatile boolean stop = false;
    private static TransactionFactory.type TYPE;

    static class ClientThread extends Thread
    {
        Database<Integer,Integer> _db;
        TransactionFactory.type TYPE;
        int _opsCommit;
        int _opsAbort;
        int _threadid;

        int rw = global_rw;
        int nread = global_nread;
        int nwrite = global_nwrite;
        int conflict_prob = global_conflict_prob > 0 ? 1 : 0;

        int min_private_key;
        int max_private_key;
        int min_shared_key;
        int max_shared_key;

        public ClientThread(Database<Integer,Integer> db, int threadid)
        {
            _db=db;
            _opsCommit=0;
            _opsAbort=0;
            _threadid = threadid;
            TYPE = MicroTimeout.TYPE;
        }

        public int getOpsCommit() {
            return _opsCommit;
        }

        public int getOpsAbort() {
            return _opsAbort;
        }

        public void run()
        {
            min_private_key = global_txn_size*_threadid;
            max_private_key = min_private_key + global_txn_size;

            min_shared_key = global_txn_size*global_num_threads;
            max_shared_key = min_shared_key + global_store_size;

            // test run
            while(!stop) {
                Vector<Integer> keys = generate_keys();
                execute_tx(keys);
            }

        }

        Vector<Integer> generate_keys(){
            Vector<Integer> keys = new Vector<>();
            int nprivate_keys = rw+nread+nwrite;
            int key;
            for (int i = 0; i < nprivate_keys; i ++){
                key = ThreadLocalRandom.current().nextInt(min_private_key, max_private_key);
                keys.add(key);
            }
            return keys;
        }

        void execute_tx(Vector<Integer> keys) {
            int v;
            try {

                TransactionAbst<Integer,Integer> t = _db.newTransaction(TYPE);

                if (conflict_prob == 1) {
                    int key_shared = ThreadLocalRandom.current().nextInt(min_shared_key, max_shared_key);
                    // read-modify-write global store
                    v = t.get(0,key_shared);
                    t.put(0,key_shared, v + 1);
                }

                int i = 0;
                for (; i < rw; i++) {
                    v = t.get(0,keys.get(i));
                    t.put(0,keys.get(i), v + 1);
                }

                for (; i < nread; i++) {
                    t.get(0,keys.get(i));
                }

                for (; i < nwrite; i++) {
                    t.put(0,keys.get(i), i + 1);
                }

                if (t.commit()) {
                    _opsCommit++;
                } else {
                    _opsAbort++;
                }

            } catch (TransactionAbortException ta){
                _opsAbort++;
            } catch (TransactionTimeoutException tt){
                _opsAbort++;
            }
        }
    }

    static void displayUsage () {
        System.out.println("Usage: MicroTimeout -alg ALGORITHM [-d duration(s)] [-t num_threads] [-s transaction-size] " +
                "[-c conflict-probability] [-r read-percentage]");
        System.exit(0);
    }

    private static int duration;

    private static int global_num_threads;
    private static int global_conflict_prob;
    private static int global_store_size;
    private static int global_txn_size;
    private static int global_read_perc;

    private static int global_rw;
    private static int global_nread;
    private static int global_nwrite;

    private static String global_algorithm;

    public static void main(String[] args) throws InterruptedException {

        parseArguments(args);
        initBench();

        System.out.println("Number of threads = "+ global_num_threads);
        System.out.println("Conflict Probability = "+ global_conflict_prob+"%");
        System.out.println("Number of shared keys  = "+ global_store_size);
        System.out.println("Number of private keys = "+ global_txn_size);
        System.out.println("Percentage of read operations = "+ global_read_perc+"%");

//        System.out.println();
//        System.out.println("RW = "+global_rw);
//        System.out.println("Read = "+global_nread);
//        System.out.println("Write = "+global_nwrite);

        TYPE = TransactionTypeFactory.getType(global_algorithm);
        Database<Integer,Integer> db= new Database<>();
        Storage storage = new MultiHashMapStorage<>();
        db.setStorage(storage);
        switch (TYPE){
            case SI:
                db.startThreadCleaner(new ThreadCleanerSI(db, db.getStorage()));
                break;
            case OCC_MULTI:
            case NMSI:
            case NMSI_ARRAY:
            case NMSI_TIMEOUT:
                db.startThreadCleaner(new ThreadCleanerNMSI<>(db, db.getStorage()));
                break;
        }
        loadDatabase(db);

        Vector<Thread> threads = new Vector<Thread>();
        for (int threadid = 0; threadid < global_num_threads; threadid++) {
            Thread t = new ClientThread(db, threadid);
            threads.add(t);
        }

        System.out.println();

        long st = System.currentTimeMillis();

        for (Thread t : threads)
            t.start();

        Thread.sleep(duration);
        stop = true;

        for (Thread t : threads)
            t.join();

        long en=System.currentTimeMillis();


        int opsCommit = 0;
        int opsAbort = 0;

        for (Thread t : threads) {
            try {
                t.join();
                opsCommit += ((ClientThread) t).getOpsCommit();
                opsAbort += ((ClientThread) t).getOpsAbort();
            } catch (InterruptedException e) {
            }
        }

        db.cleanup();

        long runtime = en-st;
        System.out.println("RunTime(ms) = "+ runtime);
        double throughput = 1000.0 * ((double) opsCommit) / ((double) runtime);
        System.out.println("Throughput(ops/sec) = " + String.format("%,f", throughput));
        System.out.println("Number Commits = "+ String.format("%,d", opsCommit));
        System.out.println("Number Aborts = "+opsAbort);
        System.out.println("Abort rate = "+Math.round((opsAbort/(double)(opsCommit+opsAbort))*100)+"%");
        System.out.println("");
    }

    private static void loadDatabase(Database<Integer,Integer> db) {

        int total_size = global_txn_size*global_num_threads + global_store_size;

        TransactionAbst<Integer,Integer> t = db.newTransaction(TYPE);

        for (int i = 0; i < total_size; i++){
            t.put(0,i, 0);
        }

        if(!t.commit()){
            System.err.println("Failed Loading Database.");
            System.exit(0);
        }

    }

    private static void initBench(){
        int i, nread, nwrite, global_writes;

        global_store_size = global_conflict_prob == 0 ? 0 : 100 / global_conflict_prob;
        global_writes = global_store_size > 0 ? 1 : 0;


        nread = (int) Math.round(((global_txn_size-global_writes) * global_read_perc) / 100.0);
        nwrite = (global_txn_size-global_writes) - nread;

        global_rw = nread > nwrite ? nwrite : nread;
        global_nread = nread > nwrite ? nread-nwrite : 0;
        global_nwrite = nread > nwrite ? 0 : nwrite-nread;

    }

    private static void parseArguments(String args[]) {
        //parse arguments
        int argindex=0;

        System.out.print("Arguments: ");
        for (String s : args)
            System.out.print(s + " ");
        System.out.println("\n");

        if (args.length==0)
        {
            displayUsage();
            System.exit(0);
        }

        while (args[argindex].startsWith("-")) {
            if (args[argindex].compareTo("-alg") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                global_algorithm = args[argindex];
                argindex++;
            }
            else if (args[argindex].compareTo("-d") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                duration = Integer.parseInt(args[argindex])*1000;
                argindex++;
            }
            else if (args[argindex].compareTo("-t") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_num_threads = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-s")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_txn_size = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-c")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_conflict_prob = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-r")==0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_read_perc = ttarget;
                argindex++;
            }
            else
            {
                System.out.println("Unknown option "+args[argindex]);
                displayUsage();
                System.exit(0);
            }

            if (argindex>=args.length)
            {
                break;
            }
        }
    }
}
