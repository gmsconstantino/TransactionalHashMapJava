package fct.thesis.database;

/**
 * Created by gomes on 14/04/15.
 */
public class DatabaseFactory {

    public static <K,V> fct.thesis.database.Database createDatabase(TransactionFactory.type t){
        Database db = new fct.thesis.database.Database<K,V>();
        switch (t){
            case SI:
                db.startThreadCleaner(new ThreadCleanerSI());
                break;
            case OCC_MULTI:
            case NMSI:
                db.startThreadCleaner(new ThreadCleanerNMSI<>(db));
                break;
        }
        return db;
    }

    public static <K,V> fct.thesis.database.Database createDatabase(TransactionFactory.type t, int threads){
        Database db = new fct.thesis.database.Database<K,V>(threads);
        switch (t){
            case SI:
                db.startThreadCleaner(new ThreadCleanerSI());
                break;
            case OCC_MULTI:
            case NMSI:
//                db.startThreadCleaner(new ThreadCleanerSI());
                db.startThreadCleaner(new ThreadCleanerNMSI<>(db));
                break;
        }
        return db;
    }

}
