package bench;

import fct.thesis.database.*;
import thrift.DatabaseSingleton;
import thrift.TransactionTypeFactory;

import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by gomes on 11/04/15.
 */
public class Micro {

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
        int num_operations = global_num_operations;

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
            TYPE = Micro.TYPE;
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
            for (int i=0; i < num_operations; i++) {
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
            int key_shared = ThreadLocalRandom.current().nextInt(min_shared_key, max_shared_key);

            Transaction<Integer,Integer> t = _db.newTransaction(TYPE);

            try {


                // read-modify-write global store
                int v = t.get(key_shared);
                t.put(key_shared, v + 1);

                int i = 0;
                for (; i < rw; i++) {
                    v = t.get(keys.get(i));
                    t.put(keys.get(i), v + 1);
                }

                for (; i < nread; i++) {
                    t.get(keys.get(i));
                }

                for (; i < nwrite; i++) {
                    t.put(keys.get(i), i + 1);
                }

                if (t.commit()) {
                    _opsCommit++;
                }

            } catch (TransactionAbortException ta){
                _opsAbort++;
            } catch (TransactionTimeoutException tt){
                _opsAbort++;
            }
        }
    }

    static void displayUsage () {
        System.out.println("Usage: Micro -alg ALGORITHM [-t num_threads] [-n num_operations] [-s transaction-size] [-c conflict-probability] [-r read-percentage]");
        System.exit(0);
    }

    private static int global_num_threads;
    private static int global_num_operations;
    private static int global_conflict_prob;
    private static int global_store_size;
    private static int global_txn_size;
    private static int global_read_perc;

    private static int global_rw;
    private static int global_nread;
    private static int global_nwrite;

    private static String global_algorithm;

    public static void main(String[] args) {

        parseArguments(args);
        initBench();

        System.out.println("Number of threads = "+ global_num_threads);
        System.out.println("Number of operations per thread = "+ global_num_operations);
        System.out.println("Conflict Probability = "+ global_conflict_prob+"%");
        System.out.println("Number of shared keys  = "+ global_store_size);
        System.out.println("Number of private keys = "+ global_txn_size);
        System.out.println("Percentage of read operations = "+ global_read_perc+"%");

//        System.out.println();
//        System.out.println("RW = "+global_rw);
//        System.out.println("Read = "+global_nread);
//        System.out.println("Write = "+global_nwrite);

        TYPE = TransactionTypeFactory.getType(global_algorithm);
        Database<Integer,Integer> db= DatabaseFactory.createDatabase(TYPE);
        loadDatabase(db);

        Vector<Thread> threads = new Vector<Thread>();
        for (int threadid = 0; threadid < global_num_threads; threadid++) {
            Thread t = new ClientThread(db, threadid);
            threads.add(t);
        }

        System.out.println();

        long st = System.currentTimeMillis();
        for (Thread t : threads) {
            t.start();
        }

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

        long en=System.currentTimeMillis();

        db.cleanup();

        long runtime = en-st;
        System.out.println("RunTime(ms) = "+ runtime);
        double throughput = 1000.0 * ((double) opsCommit) / ((double) runtime);
        System.out.println("Throughput(ops/sec) = " + throughput);
        System.out.println("Number Commits = "+opsCommit);
        System.out.println("Number Aborts = "+opsAbort);

    }

    private static void loadDatabase(Database<Integer,Integer> db) {

        int total_size = global_txn_size*global_num_threads + global_store_size;

        Transaction<Integer,Integer> t = db.newTransaction(TYPE);

        for (int i = 0; i < total_size; i++){
            t.put(i, 0);
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
            else if (args[argindex].compareTo("-n")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_num_operations = ttarget;
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
