package bench.tpcc;

import fct.thesis.database.TransactionFactory;
import fct.thesis.database.TransactionTypeFactory;
import net.openhft.affinity.AffinityLock;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Constantino Gomes on 12/05/15.
 */
public class TpccEmbeded {

    public static volatile boolean stop = false;
    public static AtomicInteger signal = new AtomicInteger(0);
    public static boolean DEBUG = false;

    private static int numClientPerWare = -1;
    private static int numWare = -1;
    private static int measureTime = -1;
    private static boolean bindWarehouse = false;

    public static void main(String[] args) throws InterruptedException {

        System.out.print("Arguments: ");
        int n = args.length-1;
        for(String arg : args){
            arg += (n==0)?"\n":", ";
            System.out.print(arg);
            n--;
        }

        parseArguments(args);


        TpccThread[] workers = new TpccThread[numClientPerWare*numWare];
        // Start each client.
        for (int i = 0; i < (numClientPerWare*numWare); i++) {
            TpccThread worker = new TpccThread(i+1, numWare, bindWarehouse);
            workers[i] = worker;
        }

        System.out.println("TPCC Data Load Started...");
        long start = System.currentTimeMillis();
        // load global porque é para todas as threads
        TpccLoad.LoadItems();

        //para cada thread coloca-la a fazer o load da bd
        for (int i = 0; i < numClientPerWare; i++) {
            workers[i].start();
        }

        while (signal.get() < numClientPerWare);

        DecimalFormat df = new DecimalFormat("#,##0.00");
        System.out.println("Load time (s): " + df.format((System.currentTimeMillis() - start) / 1000));

        printHeapSize();

        int size = 0;
        for (int i = 0; i < numWare + 1; i++) {
            size += Environment.getSizeDatabase(i);
        }
        System.out.println("Size database = "+size);

        System.out.println("\nThe assignment of CPUs is\n" + AffinityLock.dumpLocks());


        // TODO: Remove scanner only debug
//        Scanner in = new Scanner(System.in);
//        in.nextLine();

//        measure time
        System.out.printf("\nSTART BENCHMARK.\n\n");

//        loop for the measure_time
        df = new DecimalFormat("0.00");
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < numClientPerWare; i++) {
            workers[i].start = true;
        }

        Thread.sleep(measureTime*1000);
        stop = true;

        for (int i = 0; i < numClientPerWare; i++) {
            workers[i].join();
        }

        final long actualTestTime = System.currentTimeMillis() - startTime;
        System.out.println();

        Environment.cleanup();

        int totCommits = 0;
        int totAborts = 0;
        double latency = 0;
        for (TpccThread tpccThread : workers){
            totCommits += tpccThread.commits;
            totAborts += tpccThread.aborts;
            latency = latency==0? tpccThread.latency : (latency+tpccThread.latency)/2;
        }

        System.out.println("RunTime(s) = "+ df.format(actualTestTime / 1000.0f));
        double throughput = 1000.0 * ((double) totCommits) / ((double) actualTestTime);
        System.out.println("Throughput(ops/sec) = " + throughput);
        System.out.println("Number Commits = "+totCommits);
        System.out.println("Number Aborts = "+totAborts);
        System.out.println("Abort rate = "+Math.round((totAborts/(double)(totCommits+totAborts))*100)+"%");
        System.out.println("Latency (ms) = "+ df.format(latency));

        size = 0;
        for (int i = 0; i < numWare + 1; i++) {
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
                numWare=Integer.parseInt(args[argindex]);
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

        if(type!=null && numWare != -1) {
            Environment.setTransactiontype(type, numWare+1);
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
