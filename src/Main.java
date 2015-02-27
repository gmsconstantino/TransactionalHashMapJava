import database.Database;
import database.Transaction;
import database.TransactionTimeoutException;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class Main {

    static Database<Integer,Integer> db;
    static ConcurrentHashMap<Integer, Integer> concurrentHashMap;
    static List<Log> synclog;
    static Set<Transaction> commitTransactions;

    static class Log {
        Transaction t;
        int prod;
        int qtd;

        Log(Transaction t, int prod, int qtd){
            this.t = t;
            this.prod = prod;
            this.qtd = qtd;
        }
    }

    public static void populate_database(){
        Random r = new Random(0);

        Transaction t = db.txn_begin();

        for (int i = 0; i < 1000; i++) {
            int p = r.nextInt(1000);
            int q = r.nextInt(50);
            db.put(t,p, q);
            concurrentHashMap.put(p,q);
        }

        if(db.txn_commit(t)){
            commitTransactions.add(t);
        }
    }

    public static void main(String[] args) {

        db = new Database<Integer, Integer>();
        concurrentHashMap = new ConcurrentHashMap<Integer, Integer>();
        synclog = Collections.synchronizedList(new ArrayList<Log>());
        commitTransactions = new ConcurrentSkipListSet<Transaction>();

        populate_database();

//        Iterator it = db.getIterator();
//        while (it.hasNext()){
//            Map.Entry<Integer,Integer> e = (Map.Entry<Integer,Integer>) it.next();
//            System.out.println(e.getKey()+"->"+e.getValue());
//        }

        int n_threads = 10;
        Thread[] arrThreads = new Thread[n_threads];

        for (int i = 0; i <n_threads; i++) {
            arrThreads[i] = new Thread(){
                @Override
                public void run() {
                    super.run();
                    Random r = new Random();

                    try {

                        Transaction t = db.txn_begin();

                        for (int i = 0; i < 10; i++) {

                            int op = r.nextInt(100);
                            int prod_id = r.nextInt(1000);
                            if (op < 25) {
                                // probability 25% of trying to insert 'prod_id'
                                int qtd = r.nextInt(25);
                                db.put(t, prod_id, qtd);
                                synclog.add(new Log(t, prod_id, qtd));
                            } else {
                                // probability 75% of trying to lookup for 'prod_id'
                                Integer q = db.get(t, prod_id);

                                if (q == null)
                                    continue;

                                if (r.nextBoolean()) {
//                                    int qtd = q + (r.nextInt(5) * (r.nextBoolean() ? 1 : -1));
//                                    db.put(t, prod_id, qtd);
//                                    synclog.add(new Log(t, prod_id, qtd));
                                }
                            }
                        }

                        if (db.txn_commit(t)) {
                            commitTransactions.add(t);
                        }

                    }catch (TransactionTimeoutException e){
                        e.printStackTrace();
                    }
                }
            };
            arrThreads[i].start();
        }

        for (int i = 0; i < n_threads; i++) {
            try {
                arrThreads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // TODO: verify data;
        verify_data();

        System.out.println("Commit Transactions: "+commitTransactions.size());


    }

    private static void verify_data() {

        for (Log log : synclog){
            if (commitTransactions.contains(log.t)){
                concurrentHashMap.put(log.prod,log.qtd);
            }
        }

        Iterator t = db.getIterator();

        while (t.hasNext()){
            Map.Entry<Integer,Integer> entry = (Map.Entry<Integer,Integer>) t.next();

            Integer data = concurrentHashMap.get(entry.getKey());

            if (data != entry.getValue()){
                System.out.println("Error - key: "+entry.getKey()+" db: "+entry.getValue()+" hs: "+data);
            }
        }


    }
}
