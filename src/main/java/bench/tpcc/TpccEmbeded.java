package bench.tpcc;

import org.apache.commons.cli.*;
import thrift.TransactionTypeFactory;

import java.text.DecimalFormat;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Constantino Gomes on 12/05/15.
 */
public class TpccEmbeded {

    public static volatile boolean stop = false;
    public static boolean DEBUG = false;

    public static void main(String[] args) throws InterruptedException {
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args);
        } catch (ParseException e) {
            System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "tpccLoad", options );
            System.exit(0);
        }

        int numConn = Integer.parseInt(cmd.getOptionValue("c"));
        int numWare = Integer.parseInt(cmd.getOptionValue("w"));
        int measureTime = Integer.parseInt(cmd.getOptionValue("t"));
        boolean bindWarehouse = false;

        if (cmd.hasOption("d")) {
            DEBUG = true;
            TpccLoad.option_debug = true;
        }

        if (cmd.hasOption("r")){
            Environment.remote = true;
        }

        if (cmd.hasOption("B"))
            bindWarehouse = true;

        if (cmd.hasOption("tp")){
            Environment.setTransactionype(TransactionTypeFactory.getType(cmd.getOptionValue("tp")));
        }

        TpccLoad.tpccLoadData(numWare);

        printHeapSize();

        TpccThread[] workers = new TpccThread[numConn];

        // Start each server.
        for (int i = 0; i < numConn; i++) {
            TpccThread worker = new TpccThread(i+1, numWare, numConn, bindWarehouse);
            workers[i] = worker;
        }

//        measure time
        System.out.printf("\nSTART BENCHMARK.\n\n");

//        loop for the measure_time
        DecimalFormat df = new DecimalFormat("#,##0.00");

        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < numConn; i++) {
            workers[i].start();
        }

        Thread.sleep(measureTime*1000);
        stop = true;

        for (int i = 0; i < numConn; i++) {
            workers[i].join();
        }

        final long actualTestTime = System.currentTimeMillis() - startTime;
        System.out.println();

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
        System.out.println("Latency (ms)= "+ df.format(latency));
        System.out.println("");
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder("w")
                .hasArg(true)
                .desc("number of warehouses - usually 10. Change to control the size of the database")
                .required()
                .build());
        options.addOption("d", false, "debug - produces more output");
        options.addOption("r", false, "remote DB - connect with Thrift API");
        options.addOption(Option.builder("c")
                .hasArg(true)
                .desc("number of worker threads")
                .required()
                .build());
        options.addOption(Option.builder("t")
                .hasArg(true)
                .desc("duration of the benchmark (sec)")
                .required()
                .build());
        options.addOption("tp", true, "databases transactional algorithm");
        options.addOption("B", false, "bind thread to one warehouse");
        return options;
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
