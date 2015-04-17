import fct.thesis.database.Database;
import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;

/**
 * Created by gomes on 10/03/15.
 */
public class EasyTest {

    public static void main(String[] args) {

        Database<Integer, Integer> db = new Database<Integer, Integer>();

        Transaction<Integer,Integer> t = db.newTransaction(TransactionFactory.type.OCC);

        t.put(10,5);

        t.abort();

        t = db.newTransaction(TransactionFactory.type.OCC);

        Integer n = t.get(10);

        t.commit();

    }

}
