import database.Database;
import database.Transaction;
import database.TransactionTimeoutException;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class MainShop {

    static Database<Integer,Integer> db;
    static ConcurrentHashMap<Integer, Integer> concurrentHashMap;
    static List<Log> synclog;
    static Set<Transaction> transactions;

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
            transactions.add(t);
        }
    }

    public static void main(String[] args) {

        db = new Database<Integer, Integer>();
        concurrentHashMap = new ConcurrentHashMap<Integer, Integer>();
        synclog = Collections.synchronizedList(new ArrayList<Log>());
        transactions = new ConcurrentSkipListSet<Transaction>();

        populate_database();

        int n_threads = 50;
        Thread[] arrThreads = new Thread[n_threads];

        for (int i = 0; i <n_threads; i++) {
            arrThreads[i] = new Thread(){
                @Override
                public void run() {
                    super.run();
                    Random r = new Random();
                    Transaction t = null;
                    try {

                        t = db.txn_begin();
                        transactions.add(t);

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

                                if (r.nextBoolean()) {
                                    Integer q = db.get_to_update(t, prod_id);

                                    if (q == null)
                                        continue;

                                    int qtd = q + (r.nextInt(5) * (r.nextBoolean() ? 1 : -1));
                                    db.put(t, prod_id, qtd);
                                    synclog.add(new Log(t, prod_id, qtd));
                                } else {
                                    Integer q = db.get(t, prod_id);

                                    if (q == null)
                                        continue;
                                }
                            }
                        }

                        db.txn_commit(t);

                    }catch (TransactionTimeoutException e){
                        e.printStackTrace();

                        System.out.println(t);
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

        if(verify_data()){
            System.out.println("Database....... OK");
        } else
            System.out.println("Database....... Failed");

//        System.out.println("Commit Transactions: " + transactions.size());
    }

    private static boolean verify_data() {

        boolean all_ok = true;

        Transaction[] array = new Transaction[transactions.size()];
        transactions.toArray(array);

        Arrays.sort(array, new Comparator<Transaction>() {
            @Override
            public int compare(Transaction o1, Transaction o2) {
                return Long.compare(o1.getCommit_id(), o2.getCommit_id());
            }
        });

        for (int i = 0; i < array.length; i++) {
            if(array[i].success){
                for (Log log : synclog){
                    if (log.t.getId() == array[i].getId()){
                        concurrentHashMap.put(log.prod,log.qtd);
                    }
                }
            }
        }

        // Nao pode ser utilizado o size, pq a db pode criar objectos que depois nao sejam utilizados
        // pela transacao por ter abortado
//        if (db.size() != concurrentHashMap.size()){
//            System.out.println("Sizes dont match. DB: "+db.size()+" map: "+concurrentHashMap.size());
//            all_ok = false;
////            return;
//        }
        System.out.println("Sizes -> DB: "+db.size()+" map: "+concurrentHashMap.size());

        Iterator t = db.getIterator();

        while (t.hasNext()){
            Map.Entry<Integer,Integer> entry = (Map.Entry<Integer,Integer>) t.next();

            Integer data = concurrentHashMap.get(entry.getKey());
            if (entry.getValue() == null){
                System.out.println("Warn - key:"+entry.getKey()+" was empty.");
            }else if (data != entry.getValue()){
                System.out.println("Error - key: "+entry.getKey()+" db: "+entry.getValue()+" hs: "+data);
                all_ok = false;
            }
        }

        return all_ok;
    }
}
