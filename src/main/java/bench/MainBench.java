package bench;

import fct.thesis.database.Database;
import fct.thesis.database.DatabaseFactory;
import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;
import bench.measurements.Measurements;
import bench.measurements.exporter.MeasurementsExporter;
import bench.measurements.exporter.TextMeasurementsExporter;
import bench.workload.MyTxWorkload;
import bench.workload.TerminatorThread;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

/**
 * Created by gomes on 11/04/15.
 */
public class MainBench {

    static class ClientThread extends Thread
    {
        Database _db;
        MyTxWorkload _workload;
        int _opsdone;

        public ClientThread(Database db, MyTxWorkload workload)
        {
            _db=db;
            _opsdone=0;
            _workload = workload;
        }

        public int getOpsDone()
        {
            return _opsdone;
        }

        public void run()
        {
            long st=System.currentTimeMillis();
            while (!_workload.isStopRequested())
            {
                if (!_workload.doTransaction(_db,null))
                {
                    break;
                }
                _opsdone++;
            }
        }
    }

    public static void main(String[] args) {

        Random random = new Random();
        TransactionFactory.type TYPE = TransactionFactory.type.OCC;
        Database<Integer,Integer> db = (Database<Integer,Integer>) DatabaseFactory.createDatabase(TYPE);

        int _keyspace = Integer.parseInt(args[0]);

        Transaction<Integer,Integer> tx = db.newTransaction(TYPE);
        for (int i = 1; i <= _keyspace; i++) {
            tx.put(i, random.nextInt(100000));
        }
        tx.commit();

        System.out.println("Database Size: "+db.size());

        MyTxWorkload workload = new MyTxWorkload(_keyspace);

        Properties properties = new Properties();
//        properties.put("measurementtype","timeseries");
//        properties.put("timeseries.granularity","10000");
        properties.put("measurementtype","histogram");
        properties.put("histogram.buckets","60000");
        properties.put("histogram.limit","50");
        Measurements.setProperties(properties);

        int[] test_thread = new int[]{1,2,4,8,16,32,64};

        for (int i : test_thread) {

            System.out.println("\nTest with threads "+i+"\n");

            Vector<Thread> threads = new Vector<Thread>();
            int threadcount = i;
            for (int threadid = 0; threadid < threadcount; threadid++) {

                Thread t = new ClientThread(db, workload);
                threads.add(t);
                //t.start();
            }

            //TODO: Remove Scanner
//            System.out.println("Press ENTER to continue!!");
//            Scanner in = new Scanner(System.in);
//            in.nextLine();

            long st = System.currentTimeMillis();
            for (Thread t : threads) {
                t.start();
            }

            Thread terminator = new TerminatorThread(60, threads, workload);
            terminator.start();

            int opsDone = 0;

            for (Thread t : threads) {
                try {
                    t.join();
                    opsDone += ((ClientThread) t).getOpsDone();
                } catch (InterruptedException e) {
                }
            }

            long en = System.currentTimeMillis();

            if (terminator != null && !terminator.isInterrupted()) {
                terminator.interrupt();
            }

            try {
                exportMeasurements(new Properties(), opsDone, en - st);
            } catch (IOException e) {
                e.printStackTrace();
            }

            Measurements.getMeasurements().cleanMeasurements();
            workload.resetStop();
        }
    }

    private static void exportMeasurements(Properties props, int opcount, long runtime)
            throws IOException
    {
        MeasurementsExporter exporter = null;
        String exportFile = null;
        try
        {
            // if no destination file is provided the results will be written to stdout
            OutputStream out;
            exportFile = props.getProperty("exportfile");
            if (exportFile == null)
            {
                out = System.out;
            } else
            {
                out = new FileOutputStream(exportFile, true);
            }

            // if no exporter is provided the default text one will be used
            String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.bench.measurements.exporter.TextMeasurementsExporter");
            try
            {
                exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class, Properties.class).newInstance(out, props);
            } catch (Exception e)
            {
                System.err.println("Could not find exporter " + exporterStr
                        + ", will use default text reporter.");
//                e.printStackTrace();
                exporter = new TextMeasurementsExporter(out, null);
            }

            exporter.write("OVERALL", "RunTime(ms)", runtime);
            double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
            exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

            Measurements.getMeasurements().exportMeasurements(exporter);
        } finally
        {
            exporter.flush();
            if (exportFile != null)
            {
                exporter.close();
            }
        }
    }
}
