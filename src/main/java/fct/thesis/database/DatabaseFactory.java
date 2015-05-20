package fct.thesis.database;

/**
 * Created by gomes on 14/04/15.
 */
public class DatabaseFactory {

    public static <K,V> fct.thesis.database.Database createDatabase(TransactionFactory.type t){
        Database db = new fct.thesis.database.Database<K,V>();
        switch (t){
            case OCC_MULTI:
            case SI:
            case NMSI:
                db.startThreadCleaner();
                break;
        }
        return db;
    }

}
