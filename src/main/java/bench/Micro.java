package bench;

import bench.measurements.Measurements;
import bench.measurements.exporter.MeasurementsExporter;
import bench.measurements.exporter.TextMeasurementsExporter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by gomes on 11/04/15.
 */
public class Micro {

    static class ClientThread extends Thread
    {
        DB _db;
        int _opsdone;
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

        public ClientThread(DB db, int threadid)
        {
            _db=db;
            _opsdone=0;
            _threadid = threadid;
        }

        public int getOpsDone()
        {
            return _opsdone;
        }

        public void run()
        {
            min_private_key = global_txn_size*_threadid;
            max_private_key = min_private_key + global_txn_size;

            min_shared_key = global_txn_size*global_num_threads;
            max_shared_key = min_shared_key + global_store_size;

            try {
                _db.init();
            } catch (DBException e) {
                e.printStackTrace();
                System.out.println("Failed Init Database.");
                return;
            }

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

            Set<String> fields = new HashSet<>();
            fields.add("field");

            HashMap<String, String> values = txBuildValues(fields);

            long st = System.nanoTime();
            UUID id = _db.beginTx();

            // read-modify-write global store
            _db.update(key_shared, values);

            int i = 0;
            for (; i < rw; i++) {
                _db.update(keys.get(i), values);
            }

            for (; i < nread; i++) {
                _db.read(keys.get(i), fields, new HashMap<String,String>());
            }

            for (; i < nwrite; i++) {
                _db.insert(keys.get(i), values);
            }

            if(_db.commit(id) == 0){
                _opsdone++;
                long en = System.nanoTime();
                Measurements.getMeasurements().measure("Tx", (int)((en-st)/1000));
                Measurements.getMeasurements().reportReturnCode("Tx", 0);
            } else {
                Measurements.getMeasurements().reportReturnCode("Tx", -1);
            }
        }
    }

    static HashMap<String, String> txBuildValues(Set<String> fields) {
        HashMap<String,String> values=new HashMap<String,String>(fields.size());
        for (String field : fields) {
            byte[] arr = new byte[global_objects_size];
            ThreadLocalRandom.current().nextBytes(arr);
            values.put(field, new String(arr, Charset.forName("UTF-8")));
        }
        return values;
    }

    static void displayUsage () {
        System.out.println("Usage: Micro -db CLASS_BINDING [-t num_threads] [-n num_operations] [-s transaction-size] [-c conflict-probability] [-r read-percentage] [-p name=value]");
        System.exit(0);
    }

    public static final String NUM_THREADS = "num_threads";
    public static final String NUM_OPERATIONS = "num_operations";
    public static final String TX_SIZE = "transaction_size";
    public static final String CONFLICT_PROB = "conflict_probability";
    public static final String READ_PROB = "read_percentage";
    public static final String DATABASE_CLASS = "db";

    public static final String OBJECTS_SIZE = "objects_size";
    public static final int OBJECTS_SIZE_DEFAULT = 10; //Bytes

    static Set<String> fields = new HashSet<>();
    static {
        fields.add("field");
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

    private static int global_objects_size = 10;

    public static void main(String[] args) {

        System.out.println("Usage: Micro -db CLASS_BINDING [-t num_threads] [-n num_operations] [-s transaction-size] [-c conflict-probability] [-r read-percentage] [-p name=value]");


        Bundle bundle = new Bundle();

        parseArguments(args,bundle);

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

        String dbname = bundle.getProperty("db");
        if (dbname == null){
            displayUsage();
        }

        DB db=null;
        try
        {
            db=DBFactory.newDB(dbname,bundle);
        }
        catch (UnknownDBException e)
        {
            System.out.println("Unknown DB "+dbname);
            System.exit(0);
        }
        loadDatabase(db);

        //set up measurements
        Measurements.setProperties(bundle);

        Vector<Thread> threads = new Vector<Thread>();
        for (int threadid = 0; threadid < global_num_threads; threadid++) {
            try
            {
                db=DBFactory.newDB(dbname,bundle);
            }
            catch (UnknownDBException e)
            {
                System.out.println("Unknown DB "+dbname);
                System.exit(0);
            }

            Thread t = new ClientThread(db, threadid);
            threads.add(t);
        }

        System.out.println();

        long st = System.currentTimeMillis();
        for (Thread t : threads) {
            t.start();
        }

        int opsDone = 0;

        for (Thread t : threads) {
            try {
                t.join();
                opsDone += ((ClientThread) t).getOpsDone();
            } catch (InterruptedException e) {
            }
        }

        long en=System.currentTimeMillis();


        try
        {
            exportMeasurements(bundle, opsDone, en - st);
        } catch (IOException e)
        {
            System.err.println("Could not export measurements, error: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

    }

    private static void loadDatabase(DB db) {

        int total_size = global_txn_size*global_num_threads + global_store_size;

        Set<String> fields = new HashSet<>();
        fields.add("field");

        HashMap<String, String> values = txBuildValues(fields);

        try {
            db.init();
        } catch (DBException e) {
            e.printStackTrace();
            System.err.println("Failed Loading Database.");
            System.exit(0);
        }

        UUID id = db.beginTx();
        for (int i = 0; i < total_size; i++){
            db.insert(i, values);
        }

        if(db.commit(id) != 0){
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
            String exporterStr = props.getProperty("exporter", "bench.measurements.exporter.TextMeasurementsExporter");
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



    private static void parseArguments(String args[], Properties props) {
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
            if (args[argindex].compareTo("-db") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                String ttarget=args[argindex];
                props.setProperty(DATABASE_CLASS, ttarget+"");
                argindex++;
            }
            else if (args[argindex].compareTo("-t") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                props.setProperty(NUM_THREADS, ttarget+"");
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
                props.setProperty(NUM_OPERATIONS, ttarget+"");
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
                props.setProperty(TX_SIZE, ttarget+"");
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
                props.setProperty(CONFLICT_PROB, ttarget+"");
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
                props.setProperty(READ_PROB, ttarget+"");
                global_read_perc = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-os")==0) {
                argindex++;
                if (argindex >= args.length) {
                    displayUsage();
                    System.exit(0);
                }

                int ttarget=Integer.parseInt(args[argindex]);
                props.setProperty(OBJECTS_SIZE, ttarget+"");
                global_objects_size = ttarget;
                argindex++;
            }
            else if (args[argindex].compareTo("-p")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    displayUsage();
                    System.exit(0);
                }
                int eq=args[argindex].indexOf('=');
                if (eq<0)
                {
                    displayUsage();
                    System.exit(0);
                }

                String name=args[argindex].substring(0,eq);
                String value=args[argindex].substring(eq+1);
                props.put(name,value);
                //System.out.println("["+name+"]=["+value+"]");
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
