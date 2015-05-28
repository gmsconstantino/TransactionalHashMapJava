package bench;

import fct.thesis.database.*;
import fct.thesis.database.TransactionTypeFactory;

import java.util.Random;

/**
 * @author Ricardo Dias
 * @date 08/05/15
 */
public class MicroSI {

    public static TransactionFactory.type TYPE;
    public static Database<Integer,Integer> db;
    public static int perc_read_only;
    public static int max_num_accesses;
    public static int perc_writes;

    public static volatile boolean stop = false;

    public static class Worker extends Thread {
        public int commit = 0;
        public int abort = 0;
        public Random rand = new Random();

        public void run() {
            while(!stop) {
                Tx tx = genTx(rand);
                if (tx.execute(db, TYPE))
                    commit++;
                else
                    abort++;
            }
        }
    }

    public static class Tx {
        public int[] reads;
        public int[] writes;

        public boolean execute(Database<Integer, Integer> db, TransactionFactory.type TYPE) {
            Transaction<Integer,Integer> t = db.newTransaction(TYPE);

            try {

                for (int r : reads) {
                    t.get(r);
                }

                if (writes != null) {
                    for (int w : writes) {
                        t.put(w, 3);
                    }
                }


                return t.commit();
            }
            catch (TransactionAbortException e) {
                return false;
            }
            catch (TransactionTimeoutException e) {
                return false;
            }
        }
    }


    public static Tx genTx(Random rand) {
        Tx tx = new Tx();

        int read_ops = rand.nextInt(max_num_accesses)+1;

        tx.reads = new int[read_ops];

        for (int i=0; i < read_ops; i++) {
            tx.reads[i] = i;
        }

        if (rand.nextInt(100) < perc_read_only) {
            return tx;
        }


        int write_ops = (read_ops*perc_writes)/100;
        tx.writes = new int[write_ops];
        for (int i=read_ops-write_ops,j=0; i < read_ops; i++,j++) {
            tx.writes[j] = i;
        }

        return tx;
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 6) {
            System.out.println("Params: algorithm duration num_threads perc_read_only max_num_accesses perc_writes");
            System.exit(0);
        }

//        Scanner in = new Scanner(System.in);
//        System.out.print("Press Enter:");
//        in.nextLine();

        String global_algorithm = args[0];
        int duration = Integer.parseInt(args[1]);
        int num_threads = Integer.parseInt(args[2]);
        perc_read_only = Integer.parseInt(args[3]);
        max_num_accesses = Integer.parseInt(args[4]);
        perc_writes = Integer.parseInt(args[5]);

        System.out.print("Arguments: ");
        int n = args.length-1;
        for(String arg : args){
            arg += (n==0)?"\n":", ";
            System.out.print(arg);
            n--;
        }


        TYPE = TransactionTypeFactory.getType(global_algorithm);
        db = DatabaseFactory.createDatabase(TYPE);

        for (int i=0; i < max_num_accesses; i++) {
            Transaction<Integer,Integer> t = db.newTransaction(TYPE);
            t.put(i, 4);
            if (!t.commit()) {
                throw new RuntimeException();
            }

        }

        Worker[] workers = new Worker[num_threads];

        for (int i=0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        long start = System.currentTimeMillis();

        for (int i=0; i < workers.length; i++) {
            workers[i].start();
        }


        Thread.sleep(duration);
        stop = true;


        for (int i=0; i < workers.length; i++) {
            workers[i].join();
        }

        long finish = System.currentTimeMillis();

        db.cleanup();

        int commits = 0;
        int aborts = 0;
        for (int i=0; i < workers.length; i++) {
            commits += workers[i].commit;
            aborts += workers[i].abort;
        }

        long runtime = finish-start;
        System.out.println("RunTime(ms) = "+ runtime);
        double throughput = 1000.0 * ((double) commits) / ((double) runtime);
        System.out.println("Throughput(ops/sec) = " + throughput);
        System.out.println("Number Commits = "+commits);
        System.out.println("Number Aborts = "+aborts);
        System.out.println("Abort rate = "+Math.round((aborts / (double) (commits + aborts)) * 100)+"%");
        System.out.println("");
    }
}
