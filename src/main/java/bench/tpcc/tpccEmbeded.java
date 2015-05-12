package bench.tpcc;

import org.apache.commons.cli.*;
import thrift.TransactionTypeFactory;

import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Constantino Gomes on 12/05/15.
 */
public class tpccEmbeded {

    public static volatile int activate_transaction = 0;

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

        if (cmd.hasOption("tp")){
            Environment.setTransactionype(TransactionTypeFactory.getType(cmd.getOptionValue("tp")));
        }

        tpccLoad.tpccLoadData(numWare);


        ExecutorService executor = Executors.newFixedThreadPool(numConn);

        activate_transaction = 1;

        // Start each server.
        for (int i = 0; i < numConn; i++) {
            Runnable worker = new TpccThread(i, numWare, numConn);
            executor.execute(worker);
        }

        // measure time
        System.out.printf("\nMEASURING START.\n\n");

        // loop for the measure_time
        final long startTime = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("#,##0.0");
        long runTime = 0;
        while ((System.currentTimeMillis() - startTime) < measureTime * 1000) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        activate_transaction = 0;
        final long actualTestTime = System.currentTimeMillis() - startTime;
        System.out.println("Execution time lapse: " + df.format(runTime / 1000.0f) + " seconds");

        System.out.printf("\nSTOPPING THREADS\n");
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("Timed out waiting for executor to terminate");
        }

    }

}
