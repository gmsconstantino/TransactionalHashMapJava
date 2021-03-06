import fct.thesis.database.*;
import fct.thesis.storage.MultiHashMapStorage;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class MainShop {

    static final TransactionFactory.type TYPE = TransactionFactory.type.OCC;

    static Database<Integer,Integer> db;
    static ConcurrentHashMap<Integer, Integer> concurrentHashMap;
    static List<Log> synclog;
    static Set<TransactionAbst> transactions;

    static class Log {
        TransactionAbst t;
        int prod;
        int qtd;

        Log(TransactionAbst t, int prod, int qtd){
            this.t = t;
            this.prod = prod;
            this.qtd = qtd;
        }
    }

    public static void populate_database(){
        Random r = new Random(0);

        TransactionAbst t = db.newTransaction(TYPE);

        for (int i = 0; i < 1000; i++) {
            int p = r.nextInt(1000);
            int q = r.nextInt(50);
            t.put(0, p, q);
            concurrentHashMap.put(p,q);
        }

        if(t.commit()){
            transactions.add(t);
        }
    }

    public static void main(String[] args) {

        Storage storage = new MultiHashMapStorage<>();
        db = new Database<Integer, Integer>();
        db.setStorage(storage);

        concurrentHashMap = new ConcurrentHashMap<Integer, Integer>();
        synclog = Collections.synchronizedList(new ArrayList<Log>());
        transactions = new ConcurrentSkipListSet<TransactionAbst>();

        populate_database();

        int n_threads = 50;
        Thread[] arrThreads = new Thread[n_threads];

        for (int i = 0; i <n_threads; i++) {
            arrThreads[i] = new Thread(){
                @Override
                public void run() {
                    super.run();
                    Random r = new Random();
                    TransactionAbst<Integer, Integer> t = null;
//                    try {

                        t = db.newTransaction(TYPE);
                        transactions.add(t);

                        for (int i = 0; i < 10; i++) {

                            int op = r.nextInt(100);
                            int prod_id = r.nextInt(1000);
                            if (op < 25) {
                                // probability 25% of trying to insert 'prod_id'
                                int qtd = r.nextInt(25);
                                t.put(0,prod_id, qtd);
                                synclog.add(new Log(t, prod_id, qtd));
                            } else {
                                // probability 75% of trying to lookup for 'prod_id'

                                if (r.nextBoolean()) {
                                    Integer q = 9;//t.get_to_update(prod_id);

                                    if (q == null)
                                        continue;

                                    int qtd = q + (r.nextInt(5) * (r.nextBoolean() ? 1 : -1));
                                    t.put(0,prod_id, qtd);
                                    synclog.add(new Log(t, prod_id, qtd));
                                } else {
                                    Integer q = t.get(0,prod_id);

                                    if (q == null)
                                        continue;
                                }
                            }
                        }

//                        t.commit();
                        if(!t.commit()){
                            System.out.println("[abort] "+t);
                        }
//                    }catch (TransactionTimeoutException e){
////                        e.printStackTrace();
//
//                        System.out.println("[timeout] "+t);
//                    }
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
            System.out.println("true");
        } else {
            System.out.println("Database....... Failed");
            System.out.println("false");
        }

//        System.out.println("Commit Transactions: " + transactions.size());
    }

    private static boolean verify_data() {

        boolean all_ok = true;

        TransactionAbst[] array = new TransactionAbst[transactions.size()];
        transactions.toArray(array);

        Arrays.sort(array, new Comparator<TransactionAbst>() {
            @Override
            public int compare(TransactionAbst o1, TransactionAbst o2) {
                return  (new Long(o1.getCommitId())).compareTo(o2.getCommitId());
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
        System.out.println("Sizes -> DB: "+db.size(0)+" map: "+concurrentHashMap.size());

//        Iterator t = db.getIterator();
//        while (t.hasNext()){
//            Map.Entry<Integer,Integer> entry = (Map.Entry<Integer,Integer>) t.next();
//
//            Integer data = concurrentHashMap.get(entry.getKey());
//            if (entry.getValue() == null){
//                System.out.println("Warn - key:"+entry.getKey()+" was empty.");
//            }else if (data != entry.getValue()){
//                System.out.println("Error - key: "+entry.getKey()+" db: "+entry.getValue()+" hs: "+data);
//                all_ok = false;
//            }
//        }

        return all_ok;
    }
}
