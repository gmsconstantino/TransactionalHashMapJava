package bench;

import fct.thesis.database.*;
import fct.thesis.storage.MultiHashMapStorage;

import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by gomes on 11/04/15.
 */
public class MicroStorage {

    public static volatile boolean stop = false;

    static class ClientThread extends Thread
    {
        Storage<String,Integer> _storage;
        long _ops;
        int _threadid;
        int partition;
        int min_key;
        int max_key;

        int read_perc;

        public ClientThread(Storage<String,Integer> db, int threadid, int partition, int min_key, int max_key)
        {
            _storage=db;
            _ops=0;
            _threadid = threadid;
            this.partition = partition;
            this.min_key = min_key;
            this.max_key = max_key;

            read_perc = global_read_perc;

            if(global_debug)
                System.out.println("Thread "+threadid+" Partition: "+partition+" MinKey:"+min_key+" MaxKey:"+max_key);
        }

        public long getOps() {
            return _ops;
        }

        public void run()
        {
            // test run
            while(!stop) {
                execute_tx();
            }

        }

        void execute_tx() {

            int key_shared = ThreadLocalRandom.current().nextInt(min_key, max_key);
            int op = ThreadLocalRandom.current().nextInt(0, 100);

            if (op < read_perc) {
                _storage.getKey(partition, key_shared + "");
            } else {
                _storage.putIfAbsent(partition, key_shared + "_PT", key_shared);
            }
            _ops++;
        }
    }

    private static int duration;
    private static boolean global_debug = false;

    private static int global_num_threads;
    private static int global_num_partitions;
    private static int global_partition_size;
    private static int global_share_store = 0;
    private static int global_read_perc;
    private static int global_th_per_partition;

    public static void main(String[] args) throws InterruptedException {

        parseArguments(args);

        assert (global_num_threads >= global_num_partitions);
        global_th_per_partition = global_num_threads/global_num_partitions;

        System.out.println("Number of threads = " + global_num_threads);
        System.out.println("Number of partitions = "+ global_num_partitions);
        System.out.println("Number of Partition size = "+ global_partition_size);
        System.out.println("Percentage of get operations = "+ global_read_perc+"%");
        if (global_read_perc!=100)
            System.out.println("Percentage of putIfAbsent operations = "+ (100-global_read_perc)+"%");


        Storage storage = new MultiHashMapStorage<>(global_num_partitions);
        loadStorage(storage);


        int key_range = global_partition_size / global_th_per_partition;
        int threadid = 0;
        Vector<Thread> threads = new Vector<Thread>();
        for (int j = 0; j < global_th_per_partition; j++) {
            for (int i = 0; i < global_num_partitions; i++) {
                Thread t = null;
                if (global_share_store>0)
                    t = new ClientThread(storage, threadid++, i, 0, global_partition_size);
                else
                    t = new ClientThread(storage, threadid++, i, i*key_range, (i+1)*key_range);
                threads.add(t);
            }
        }

        System.out.println();
        System.out.println("Start....");
        long st = System.currentTimeMillis();

        for (Thread t : threads)
            t.start();

        Thread.sleep(duration);
        stop = true;

        for (Thread t : threads)
            t.join();

        long en=System.currentTimeMillis();


        long ops = 0;

        for (Thread t : threads) {
            try {
                t.join();
                ops += ((ClientThread) t).getOps();
            } catch (InterruptedException e) {
            }
        }

        long runtime = en-st;
        System.out.println("RunTime(ms) = "+ runtime);
        double throughput = 1000.0 * ((double) ops) / ((double) runtime);
        System.out.println("Throughput(ops/sec) = " + String.format("%,f", throughput));
        System.out.println();
    }

    private static void loadStorage(Storage<String, Integer> storage) {

        for (int i = 0; i < global_num_partitions; i++) {
            for (int j = 0; j < global_partition_size; j++) {
                storage.putIfAbsent(i, j+"", j);
            }
        }

    }

    static void displayUsage () {
        System.out.println("Usage: MicroStorage [-d duration(s)] [-t num_threads] [-p partitions] [-sz size]" +
                "[-r read-percentage] [-sh share] [-debug]");
        System.exit(0);
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
            if (args[argindex].compareTo("-d") == 0) {
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
            else if (args[argindex].compareTo("-p")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_num_partitions = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-sz")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_partition_size = ttarget;
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
            else if (args[argindex].compareTo("-sh")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_share_store = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-debug")==0)
            {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                global_debug = ttarget>0;
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
