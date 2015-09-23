package bench.tpcc;

import fct.thesis.database.TransactionFactory;
import fct.thesis.database.TransactionTypeFactory;
import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategy;
import net.openhft.affinity.AffinityThreadFactory;
import net.openhft.affinity.CpuLayout;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.affinity.AffinityStrategies.*;

/**
 * Created by Constantino Gomes on 12/05/15.
 */
public class TpccEmbeded {

    public static volatile boolean stop = false;
    public static AtomicInteger signal = new AtomicInteger(0);
    public static AtomicInteger init_signal = new AtomicInteger(1);
    public static boolean DEBUG = false;

    private static int numClientPerWare = -1;
    private static int numWares = -1;
    private static int measureTime = -1;
    private static boolean bindWarehouse = false;

    static int index = 0;
    static int[] cpu_list = new int[64];
    static {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        if(hostname.equals("node10")){
             // cpu_list = [
             //     0, 16, 32, 48,
             //     1, 17, 33, 49,
             //     2, 18, 34, 50,
             //     3, 19, 35, 51,
             //     4, 20, 36, 52,
             //     5, 21, 37, 53,
             //     6, 22, 38, 54,
             //     7, 23, 39, 55,
             //     8, 24, 40, 56,
             //     9, 25, 41, 57,
             //     10, 26, 42, 58,
             //     11, 27, 43, 59,
             //     12, 28, 44, 60,
             //     13, 29, 45, 61,
             //     14, 30, 46, 62,
             //     15, 31, 47, 63]
            for (int i = 0, j=0, k=0; i < cpu_list.length; i++){
                cpu_list[i] = j;
                j=j+16; k++;
                if(k==4){ j=++j%16; k=0; }
            }
        } else if(hostname.equals("node9")){

        } else {
            for (int i = 0; i < 64; i++) {
                cpu_list[i] = i;
            }
        }
    }

    static final AffinityStrategy TPCC_STRATEGY = new AffinityStrategy(){
        @Override
        public boolean matches(int cpuId, int cpuId2) {
            CpuLayout cpuLayout = AffinityLock.cpuLayout();

            boolean result = cpu_list[index]==cpuId2;
            if(result)
                index++;

            return result;
        }

        @Override
        public String toString(){
            return "TPCC_STRATEGY";
        }
    };

    public static void main(String[] args) throws InterruptedException, UnknownHostException {

        System.out.print("Arguments: ");
        int n = args.length-1;
        for(String arg : args){
            arg += (n==0)?"\n":", ";
            System.out.print(arg);
            n--;
        }

        parseArguments(args);

        System.out.println("TPCC Data Load Started...");
        long start = System.currentTimeMillis();
        // load global porque e' uma tabela 'a parte
        TpccLoad.LoadItems();

        int totalWorkers = numClientPerWare * numWares;
        TpccThread[] workers = new TpccThread[totalWorkers];
        Thread[] threads = new Thread[totalWorkers];

        // Start each client.
        int n_worker = 1;

        //AffinityThreadFactory factory = new AffinityThreadFactory("worker", TPCC_STRATEGY, ANY);
        MyThreadFactory factory = new MyThreadFactory("worker", TPCC_STRATEGY, ANY);

        for (int i = 0; i < numWares; i++) {
            boolean shouldload = true;
            for (int j = 0; j < numClientPerWare; j++) {
                TpccThread worker = new TpccThread(n_worker, i+1, numWares, bindWarehouse, cpu_list[n_worker-1], shouldload);
                System.out.println("Start worker "+n_worker);
                n_worker++;
                workers[n_worker-2] = worker;

                threads[n_worker-2] = factory.newThread(worker);
                threads[n_worker-2].start();

                while(init_signal.get() < n_worker);

                shouldload = false;
            }
        }
        System.out.println("Waiting to load database...");
        while (signal.get() < totalWorkers);

        DecimalFormat df = new DecimalFormat("#,##0.00");
        System.out.println("Load time (s): " + df.format((System.currentTimeMillis() - start) / 1000));

        System.out.println();
        printHeapSize();

        int size = 0;
        for (int i = 0; i < numWares + 1; i++) {
            size += Environment.getSizeDatabase(i);
        }
        System.out.println("Size database = "+size);

        //System.out.println("\nThe assignment of CPUs is\n" + AffinityLock.dumpLocks());


        // TODO: Remove scanner only debug
//        Scanner in = new Scanner(System.in);
//        in.nextLine();

//        measure time
        System.out.printf("\nSTART BENCHMARK.\n\n");

//        loop for the measure_time
        df = new DecimalFormat("0.00");
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < totalWorkers; i++) {
            workers[i].start = true;
        }

        Thread.sleep(measureTime*1000);
        stop = true;

        for (int i = 0; i < totalWorkers; i++) {
            threads[i].join();
        }

        final long actualTestTime = System.currentTimeMillis() - startTime;
        System.out.println();

        Environment.cleanup();


        int totExpAborts = 0;
        int totCommits_neworder = 0;
        int totCommits_payment = 0;
        int totCommits_delivery = 0;
        int totCommits_orderstat = 0;
        int totCommits_stocklev = 0;
        int totAborts_neworder = 0;
        int totAborts_payment = 0;
        int totAborts_delivery = 0;
        int totAborts_orderstat = 0;
        int totAborts_stocklev = 0;

        double latency_neworder = 0;
        double latency_payment = 0;
        double latency_delivery = 0;
        double latency_orderstat = 0;
        double latency_stocklev = 0;


        for (TpccThread tpccThread : workers){
            totCommits_neworder += tpccThread.commits_neworder;
            totCommits_delivery += tpccThread.commits_delivery;
            totCommits_orderstat += tpccThread.commits_orderstat;
            totCommits_payment += tpccThread.commits_payment;
            totCommits_stocklev += tpccThread.commits_stocklev;
            totAborts_neworder += tpccThread.aborts_neworder;
            totAborts_delivery += tpccThread.aborts_delivery;
            totAborts_orderstat += tpccThread.aborts_orderstat;
            totAborts_payment += tpccThread.aborts_payment;
            totAborts_stocklev += tpccThread.aborts_stocklev;
            totExpAborts += tpccThread.explicit_aborts;

            latency_neworder += tpccThread.latency_neworder;
            latency_payment += tpccThread.latency_payment;
            latency_delivery += tpccThread.latency_delivery;
            latency_orderstat += tpccThread.latency_orderstat;
            latency_stocklev += tpccThread.latency_stocklev;
        }

        latency_neworder /= workers.length;
        latency_payment /= workers.length;
        latency_delivery /= workers.length;
        latency_orderstat /= workers.length;
        latency_stocklev /= workers.length;

        int totCommits = totCommits_delivery + totCommits_neworder + totCommits_orderstat + totCommits_payment + totCommits_stocklev;
        int totAborts = totAborts_delivery + totAborts_neworder + totAborts_orderstat + totAborts_payment + totAborts_stocklev;

        System.out.println("RunTime(s) = "+ df.format(actualTestTime / 1000.0f));
        double throughput = 1000.0 * ((double) totCommits) / ((double) actualTestTime);
        System.out.println("Throughput(ops/sec) = " + throughput);
        System.out.println("Number Commits = "+totCommits);
        System.out.println("Number Aborts = "+totAborts);
        System.out.println("Number Explicit Aborts = "+totExpAborts);
        System.out.println("Abort rate = "+Math.round((totAborts/(double)(totCommits+totAborts))*100)+"%");
        //System.out.println("Latency (ms) = "+ df.format(latency));

        System.out.println(String.format("NewOrder\t%d\t%d\t%f", totCommits_neworder, totAborts_neworder, latency_neworder/1000));
        System.out.println(String.format("Payment  \t%d\t%d\t%f", totCommits_payment, totAborts_payment, latency_payment/1000));
        System.out.println(String.format("Delivery\t%d\t%d\t%f", totCommits_delivery, totAborts_delivery, latency_delivery/1000));
        System.out.println(String.format("OrderStatus\t%d\t%d\t%f", totCommits_orderstat, totAborts_orderstat, latency_orderstat/1000));
        System.out.println(String.format("StockLevel\t%d\t%d\t%f", totCommits_stocklev, totAborts_stocklev, latency_stocklev/1000));

        size = 0;
        for (int i = 0; i < numWares + 1; i++) {
            size += Environment.getSizeDatabase(i);
        }
        System.out.println("Size database = "+size);
        System.out.println("");
    }

    private static void parseArguments(String[] args){
        //parse arguments
        int argindex=0;

        if (args.length==0)
        {
            usageMessage();
            System.exit(0);
        }

        TransactionFactory.type type = null;

        while (args[argindex].startsWith("-"))
        {
            if (args[argindex].compareTo("-w")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                numWares =Integer.parseInt(args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-c")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                numClientPerWare =Integer.parseInt(args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-t")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                measureTime=Integer.parseInt(args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-tp")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                type = TransactionTypeFactory.getType(args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-d")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                DEBUG = true;
                TpccLoad.option_debug = true;
                argindex++;
            }
            else if (args[argindex].compareTo("-v")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                TpccLoad.verbose = true;
                argindex++;
            }
            else if (args[argindex].compareTo("-r")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                Environment.remote = true;
                argindex++;
            }
            else if (args[argindex].compareTo("-B")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                bindWarehouse = true;
                argindex++;
            }
            else
            {
                System.out.println("Unknown option "+args[argindex]);
                usageMessage();
                System.exit(0);
            }

            if (argindex>=args.length)
            {
                break;
            }
        }

        if(type!=null && numWares != -1) {
            Environment.setTransactiontype(type, numWares +1);
        } else {
            usageMessage();
            System.exit(0);
        }

    }

    private static void usageMessage() {
//        usage: tpccEmbeded
//                -B          bind thread to one warehouse
//                -c <arg>    number of worker threads
//                -d          debug - produces more output
//        -r          remote DB - connect with Thrift API
//                -t <arg>    duration of the benchmark (sec)
//                -tp <arg>   databases transactional algorithm
//        -w <arg>    number of warehouses - usually 10. Change to control the size
//        of the database
    }

    public static void printHeapSize(){
        int mb = 1024*1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        System.out.println("##### Heap utilization statistics [MB] #####");

        //Print used memory
        System.out.println("Used Memory:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);

        //Print free memory
        System.out.println("Free Memory:"
                + runtime.freeMemory() / mb);

        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);

        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
    }

}
