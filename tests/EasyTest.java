import fct.thesis.database.*;

import java.util.InputMismatchException;
import java.util.IntSummaryStatistics;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by gomes on 10/03/15.
 */
public class EasyTest {

    public static void main(String[] args) {

//        Database<Integer, Integer> db = new Database<Integer, Integer>();
//
//        Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);
//
//        t.put(10,5);
//
//        t.abort();
//
//        t = db.newTransaction(TransactionFactory.type.OCC);
//
//        Integer n = t.get(10);
//
//        t.commit();

        TreeMap<Integer, BufferDb<Integer,Integer>> map = new TreeMap<>();
        BufferObjectDb<Integer,Integer> buffer = new BufferObjectDb<Integer, Integer>(4,10);
        BufferObjectDb<Integer,Integer> buffer2 = new BufferObjectDb<Integer, Integer>(4,14);

        int n = buffer.compareTo(buffer2);
        System.out.println(n);

        System.out.println(buffer.equals(buffer2));

        Random r = new Random();

        for (int i = 0; i < 10; i++) {
            int k = r.nextInt(100);
            map.put(k, new BufferObjectDb<Integer,Integer>(k,r.nextInt(1000)));
        }

        map.put(buffer.getKey(), buffer);
        map.put(buffer2.getKey(), buffer2);
        for (BufferDb<Integer,Integer> bufferDb : map.values())
            System.out.println(bufferDb.getKey()+ " -> " + bufferDb.getValue());

    }

}
