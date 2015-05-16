package bench.tpcc;

import org.apache.commons.cli.*;
import thrift.TransactionTypeFactory;

import java.text.DecimalFormat;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Constantino Gomes on 12/05/15.
 */
public class TpccEmbeded {

    public static volatile int activate_transaction = 0;
    public static boolean DEBUG = false;

    public static void main(String[] args) {
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

        if (cmd.hasOption("d")) {
            DEBUG = true;
            TpccLoad.option_debug = true;
        }

        if (cmd.hasOption("tp")){
            Environment.setTransactionype(TransactionTypeFactory.getType(cmd.getOptionValue("tp")));
        }

        Thread thread = new Thread(){
            @Override
            public void run() {
                super.run();
                TpccLoad.tpccLoadData(numWare);
            }
        };

        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ExecutorService executor = Executors.newFixedThreadPool(numConn);

        activate_transaction = 1;

        Vector<TpccThread> threads = new Vector<>();

        // Start each server.
        for (int i = 0; i < numConn; i++) {
            TpccThread worker = new TpccThread(i, numWare, numConn);
            threads.add(worker);
            executor.execute(worker);
        }

//        measure time
        System.out.printf("\nMEASURING START.\n\n");

//        loop for the measure_time
        final long startTime = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("#,##0.0");
        long runTime = 0;
        while ((System.currentTimeMillis() - startTime) < measureTime * 1000) {
            try {
                Thread.sleep(1000);
                System.out.print(".");
            } catch (InterruptedException e) {
            }
        }
        activate_transaction = 0;
        final long actualTestTime = System.currentTimeMillis() - startTime;
        System.out.println();
//        System.out.println("Execution time lapse: " + df.format(actualTestTime / 1000.0f) + " seconds");

        System.out.printf("\nSTOPPING THREADS\n");
        executor.shutdown();

        int totCommits = 0;
        int totAborts = 0;
        for (TpccThread tpccThread : threads){
            totCommits += tpccThread.commits;
            totAborts += tpccThread.aborts;
        }

        System.out.println("RunTime(s) = "+ df.format(actualTestTime / 1000.0f));
        double throughput = 1000.0 * ((double) totCommits) / ((double) actualTestTime);
        System.out.println("Throughput(ops/sec) = " + throughput);
        System.out.println("Number Commits = "+totCommits);
        System.out.println("Number Aborts = "+totAborts);
        System.out.println("Abort rate = "+Math.round((totAborts/(double)(totCommits+totAborts))*100)+"%");
        System.out.println("");
    }

}
